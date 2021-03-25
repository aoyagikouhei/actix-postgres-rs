use actix_daemon_utils::{
    actix::{prelude::*, System},
    graceful_stop::GracefulStop,
    looper::{Looper, Task},
};
use actix_postgres::{bb8_postgres::tokio_postgres::tls::NoTls, PostgresActor, PostgresMessage};
use std::time::Duration;

struct MyActor {
    msg: String,
    seconds: u64,
    pg: Addr<PostgresActor<NoTls>>,
}

impl Actor for MyActor {
    type Context = Context<Self>;
}

impl Handler<Task> for MyActor {
    type Result = Option<std::time::Duration>;

    fn handle(&mut self, _msg: Task, ctx: &mut Self::Context) -> Self::Result {
        println!("{}", self.msg);
        let task = PostgresMessage::new(|pool| {
            Box::pin(async move {
                let connection = pool.get().await?;
                connection
                    .query("SELECT NOW()::TEXT as c", &vec![])
                    .await
                    .map_err(|err| err.into())
            })
        });
        let msg2 = self.msg.clone();
        self.pg
            .send(task)
            .into_actor(self)
            .map(move |res, _act, _ctx| match res {
                Ok(res) => match res {
                    Ok(res) => {
                        let val: &str = res[0].get(0);
                        println!("{},{}", msg2, val);
                    }
                    Err(err) => println!("{:?}", err),
                },
                Err(err) => println!("{:?}", err),
            })
            .wait(ctx);
        Some(Duration::from_secs(self.seconds))
    }
}

struct MyActor2 {
    msg: String,
    seconds: u64,
    pg: Addr<PostgresActor<NoTls>>,
}

impl Actor for MyActor2 {
    type Context = Context<Self>;
}

impl Handler<Task> for MyActor2 {
    type Result = Option<std::time::Duration>;

    fn handle(&mut self, _msg: Task, ctx: &mut Self::Context) -> Self::Result {
        println!("{}", self.msg);
        let task = PostgresMessage::new(|pool| {
            Box::pin(async move {
                let connection = pool.get().await?;
                connection
                    .query_one("SELECT NOW()::TEXT as c", &vec![])
                    .await
                    .map_err(|err| err.into())
            })
        });
        let msg2 = self.msg.clone();
        self.pg
            .send(task)
            .into_actor(self)
            .map(move |res, _act, _ctx| match res {
                Ok(res) => match res {
                    Ok(res) => {
                        let val: &str = res.get(0);
                        println!("{},{}", msg2, val);
                    }
                    Err(err) => println!("{:?}", err),
                },
                Err(err) => println!("{:?}", err),
            })
            .wait(ctx);
        Some(Duration::from_secs(self.seconds))
    }
}

fn main() {
    let path = std::env::var("PG_PATH").unwrap();
    let sys = System::new();
    let graceful_stop = GracefulStop::new();
    sys.block_on(async {
        let pg_actor = PostgresActor::start(&path, NoTls).unwrap();
        let actor1 = MyActor {
            msg: "x".to_string(),
            seconds: 1,
            pg: pg_actor.clone(),
        }
        .start();
        let looper1 =
            Looper::new(actor1.recipient(), graceful_stop.clone_system_terminator()).start();
        let actor2 = MyActor2 {
            msg: "y".to_string(),
            seconds: 1,
            pg: pg_actor,
        }
        .start();
        let looper2 =
            Looper::new(actor2.recipient(), graceful_stop.clone_system_terminator()).start();
        graceful_stop
            .subscribe(looper1.recipient())
            .subscribe(looper2.recipient())
            .start();
    });

    let _ = sys.run();
    println!("main terminated");
}
