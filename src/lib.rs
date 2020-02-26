use actix::prelude::*;
use bb8_postgres::{
    bb8::{Pool},
    PostgresConnectionManager,
    tokio_postgres::{
        config::Config,
        error::Error,
        row::Row,
        Socket,
        tls::{
            MakeTlsConnect,
            TlsConnect,
        },
    },
};
use std::str::FromStr;
use std::marker::Unpin;
use std::marker::PhantomData;

pub use bb8_postgres;

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
    pub fn start(path: &str, tls: Tls) -> Result<Addr<PostgresActor<Tls>>, Error>
    {
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

    fn started(&mut self, ctx: &mut Context<Self>)
    {
        let mgr = PostgresConnectionManager::new(self.config.clone(), self.tls.clone());
        Pool::builder()
            .build(mgr)
            .into_actor(self)
            .then(|res, act, _ctx|{
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

#[derive(Message)]
#[rtype(result = "Result<Vec<Row>, Error>")]
pub struct QueryTask<F,Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
    F: FnOnce(Pool<PostgresConnectionManager<Tls>>) -> ResponseFuture<Result<Vec<Row>, Error>> + 'static,
{
    query: F,
    phantom: PhantomData<Tls>,
}

impl<F, Tls> QueryTask<F, Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
    F: FnOnce(Pool<PostgresConnectionManager<Tls>>) -> ResponseFuture<Result<Vec<Row>, Error>> + 'static + Send + Sync,
{
    pub fn new(query: F) -> Self {
        Self {
            query: query,
            phantom: PhantomData,
        }
    }
}

impl<F, Tls> Handler<QueryTask<F, Tls>> for PostgresActor<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + Unpin,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send + Unpin,
    F: FnOnce(Pool<PostgresConnectionManager<Tls>>) -> ResponseFuture<Result<Vec<Row>, Error>> + 'static + Send + Sync,
{
    type Result = ResponseFuture<Result<Vec<Row>, Error>>;

    fn handle(&mut self, msg: QueryTask<F, Tls>, _ctx: &mut Self::Context) -> Self::Result
    {
        let pool2 = self.pool.as_ref().unwrap().clone();
        Box::pin(async move {
            (msg.query)(pool2).await
        })
    }
}
