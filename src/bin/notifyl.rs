use sqlx::postgres::{PgListener, PgNotification, PgPoolOptions};
use sqlx::{Error, Executor};

// Notify-listen with SQL (not to be considered since notification capabilities are capable of missing events)
#[tokio::main]
async fn main() -> eyre::Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://outbox:outbox@localhost/outbox")
        .await?;

    let channel = "virtual";
    let query = format!("NOTIFY {channel}, 'This is the first payload';");
    pool.acquire()
        .await?
        .execute(query.as_str())
        .await?;
    let mut listener = PgListener::connect("postgres://outbox:outbox@localhost/outbox").await?;
    listener.listen(channel).await?;

    let query = format!("NOTIFY {channel}, 'This is the second payload';");
    pool.acquire()
        .await?
        .execute(query.as_str())
        .await?;

    let query = format!("NOTIFY {channel}, 'This is the third payload';");
    pool.acquire()
        .await?
        .execute(query.as_str())
        .await?;

    loop {
        match listener.recv().await {
            Ok(notification) => {
                println!("{notification:?}");
            }
            Err(error) => {
                println!("{error}");
            }
        }
    }
}