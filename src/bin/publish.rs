use std::time::Duration;

use outbox::Rabbit;
use sqlx::types::Json;
use sqlx::PgPool;
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
        .execute("DROP TABLE IF EXISTS messages_outbox")
        .await?;

    pool.acquire()
        .await?
        .execute(
            r#"
        CREATE TABLE messages_outbox (
            id SERIAL,
            payload JSON NOT NULL,
            relayed_at TIMESTAMP DEFAULT null
        )"#,
        )
        .await?;

    tokio::spawn(relay_to_rabbit(pool.clone()));

    let mut count = 0;
    loop {
        println!("Starting to publish...");
        sqlx::query("INSERT INTO messages_outbox (payload) VALUES ($1)")
            .bind(Json(serde_json::json!({ "key": count })))
            .execute(&pool)
            .await?;
        println!("Published {count}");
        count += 1;
        sleep(Duration::from_millis(500)).await;
    }
}

#[derive(Clone, Debug, sqlx::FromRow)]
struct Message {
    id: i32,
    payload: Json<serde_json::Value>,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
struct Messages(pub Vec<Message>);

impl Messages {
    fn to_ids_list(&self) -> String {
        self.0
            .clone()
            .into_iter()
            .map(|message| message.id.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

async fn relay_to_rabbit(pool: PgPool) -> Result<(), eyre::Error> {
    println!("Starting relay...");

    let rabbit = Rabbit::new("amqp://outbox:outbox@localhost:5672/%2f")
        .await
        .map_err(|err| dbg!(err))?;

    loop {
        let messages: Vec<Message> =
            sqlx::query_as("SELECT id, payload FROM messages_outbox WHERE relayed_at IS NULL")
                .fetch_all(&pool)
                .await
                .map_err(|err| dbg!(err))?;
        let messages = Messages(messages);

        if messages.is_empty() {
            continue;
        }

        for message in messages.clone().0 {
            rabbit
                .dispatch("relayed-messages", message.payload.0)
                .await?;
        }

        let sql_query = format!(
            "UPDATE messages_outbox SET relayed_at = now() WHERE id IN ({})",
            messages.to_ids_list()
        );

        println!("Relayed {}", messages.to_ids_list());
        sqlx::query(&sql_query)
            .execute(&pool)
            .await
            .map_err(|err| dbg!(err))?;
    }
}
