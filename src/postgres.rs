use actix::prelude::*;
use bb8_postgres::{
    bb8::{Pool, RunError},
    tokio_postgres::{
        config::Config,
        error::Error,
        tls::{MakeTlsConnect, TlsConnect},
        Socket,
    },
    PostgresConnectionManager,
};
use std::marker::PhantomData;
use std::marker::Unpin;
use std::str::FromStr;
use thiserror::Error;

/// PostgreSQL Actor
pub struct PostgresActor<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
{
    config: Config,
    tls: Tls,
    pool: Option<Pool<PostgresConnectionManager<Tls>>>,
}

impl<Tls> PostgresActor<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
{
    /// Start new Supervisor with PostgresActor
    pub fn start(path: &str, tls: Tls) -> Result<Addr<PostgresActor<Tls>>, Error> {
        let config = Config::from_str(path)?;
        Ok(Supervisor::start(|_| PostgresActor {
            config: config,
            tls: tls,
            pool: None,
        }))
    }
}

impl<Tls> Actor for PostgresActor<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        let mgr = PostgresConnectionManager::new(self.config.clone(), self.tls.clone());
        Pool::builder()
            .build(mgr)
            .into_actor(self)
            .then(|res, act, _ctx| {
                act.pool = Some(res.unwrap());
                async {}.into_actor(act)
            })
            .wait(ctx);
    }
}

impl<Tls> Supervised for PostgresActor<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
{
    fn restarting(&mut self, _: &mut Self::Context) {
        self.pool.take();
    }
}

/// PostgreSQL Message
#[derive(Debug)]
pub struct PostgresMessage<F, Tls, R>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
    F: FnOnce(Pool<PostgresConnectionManager<Tls>>) -> ResponseFuture<Result<R, PostgresError>>
        + 'static,
{
    query: F,
    phantom: PhantomData<Tls>,
}

impl<F, Tls, R> Message for PostgresMessage<F, Tls, R>
where
    R: 'static,
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
    F: FnOnce(Pool<PostgresConnectionManager<Tls>>) -> ResponseFuture<Result<R, PostgresError>>
        + 'static
        + Send
        + Sync,
{
    type Result = Result<R, PostgresError>;
}

impl<F, Tls, R> PostgresMessage<F, Tls, R>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
    F: FnOnce(Pool<PostgresConnectionManager<Tls>>) -> ResponseFuture<Result<R, PostgresError>>
        + 'static
        + Send
        + Sync,
{
    pub fn new(query: F) -> Self {
        Self {
            query: query,
            phantom: PhantomData,
        }
    }
}

impl<F, Tls, R> Handler<PostgresMessage<F, Tls, R>> for PostgresActor<Tls>
where
    R: 'static + Send,
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
    F: FnOnce(Pool<PostgresConnectionManager<Tls>>) -> ResponseFuture<Result<R, PostgresError>>
        + 'static
        + Send
        + Sync,
{
    type Result = ResponseFuture<Result<R, PostgresError>>;

    fn handle(
        &mut self,
        msg: PostgresMessage<F, Tls, R>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        if let Some(pool) = &self.pool {
            let pool = pool.clone();
            Box::pin(async move { (msg.query)(pool).await })
        } else {
            Box::pin(async { Err(PostgresError::PoolNone) })
        }
    }
}

/// PostgreSQL Errors
#[derive(Debug, Error)]
pub enum PostgresError {
    /// An error returned from postgres.
    #[error(transparent)]
    PgError(#[from] Error),
    /// An error returned from bb8.
    #[error(transparent)]
    BB8Error(#[from] RunError<Error>),
    /// An error returned at pool none.
    #[error("connection pool not initialized")]
    PoolNone,
    /// An error returned from user.
    #[error("user error: {0}")]
    Other(String),
}
