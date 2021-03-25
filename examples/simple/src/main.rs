use actix::prelude::*;
use actix_postgres::{bb8_postgres::tokio_postgres::tls::NoTls, PostgresActor, PostgresMessage};

#[actix::main]
async fn main() {
    let path = std::env::var("PG_PATH").unwrap();
    let pg_actor = PostgresActor::start(&path, NoTls).unwrap();
    let task = PostgresMessage::new(|pool| {
        Box::pin(async move {
            let connection = pool.get().await?;
            connection
                .query("SELECT NOW()::TEXT as c", &vec![])
                .await
                .map_err(|err| err.into())
        })
    });
    let res = pg_actor.send(task).await.unwrap().unwrap();
    let val: &str = res[0].get(0);
    println!("{}", val);
    System::current().stop();
}
