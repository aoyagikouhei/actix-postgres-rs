//! PostgreSQL integration for Actix framework.
pub use bb8_postgres;

mod postgres;
pub use postgres::{PostgresActor, PostgresError, PostgresMessage};
