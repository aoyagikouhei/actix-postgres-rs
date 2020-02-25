use actix::prelude::*;
use actix_daemon_utils::{
    graceful_stop::{GracefulStop},
    looper::{Looper, Task},
};
use actix_postgres::{
    bb8_postgres::tokio_postgres::tls::NoTls,
    PostgresActor,
    PostgresTask,
};

struct MyActor { msg: String, seconds: u64, pg: Addr<PostgresActor<NoTls>> }

impl Actor for MyActor {
    type Context = Context<Self>;
}

impl Handler<Task> for MyActor {
    type Result = u64;

    fn handle(&mut self, _msg: Task, _ctx: &mut Self::Context) -> Self::Result {
        println!("{}", self.msg);
        let msg2 = self.msg.clone();
        let task = PostgresTask::new(
            |pool| Box::pin(async move {
                let connection = pool.get().await.unwrap();
                let res = connection.query("SELECT NOW()::TEXT as c", &vec![]).await.unwrap();
                let val: &str = res[0].get(0);
                println!("{},{}", msg2, val);
            }));
        self.pg.do_send(task);
        self.seconds
    }
}

fn main() {
    let path = std::env::var("PG_PATH").unwrap();
    let sys = actix::System::new("main");
    let graceful_stop = GracefulStop::new();
    let pg_actor = PostgresActor::start(&path, NoTls).unwrap();
    let actor1 = MyActor { msg: "x".to_string(), seconds: 1, pg: pg_actor }.start();
    let looper1 = Looper::new(actor1.recipient(), graceful_stop.clone_system_terminator()).start();
    graceful_stop
        .subscribe(looper1.recipient())
        .start();

    let _ = sys.run();
    println!("main terminated");
}
