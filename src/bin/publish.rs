use std::time::Duration;

use sqlx::types::Json;
use sqlx::{postgres::PgPoolOptions, Executor};
use tokio::time::sleep;

// STRATEGIES
// 1. polling

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://outbox:outbox@localhost/outbox")
        .await?;

    pool.acquire()
        .await?
        .execute("DROP TABLE messages_outbox")
        .await?;

    pool.acquire()
        .await?
        .execute(
            r#"
        CREATE TABLE messages_outbox (
            payload JSON NOT NULL,
            relayed_at TIMESTAMP DEFAULT null
        )"#,
        )
        .await?;

    let mut count = 0;
    loop {
        sqlx::query("INSERT INTO messages_outbox (payload) VALUES ($1)")
            .bind(Json(serde_json::json!({ "key": count })))
            .execute(&pool)
            .await?;
        println!("Published {count}");
        count += 1;
        sleep(Duration::from_millis(500)).await;
    }
}

fn relay_to_rabbit() {}
