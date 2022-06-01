use eyre::bail;
// use crate::backend::{Backend, MessageServiceBackend};
// use crate::{Dispatchable, MessageReceipt, MessageService, MessageServiceError, MessageServiceResult};
// use futures::StreamExt;
use lapin::{
    options::{BasicPublishOptions, BasicQosOptions},
    BasicProperties, Connection, ConnectionProperties,
};

pub struct Rabbit {
    channel: lapin::Channel,
}

impl Rabbit {
    /// Tries to construct a new instance of Rabbit by connecting to the specified url.
    pub async fn new(url: &str) -> eyre::Result<Self> {
        let connection = Connection::connect(url, ConnectionProperties::default()).await?;

        let channel = connection.create_channel().await?;

        // https://www.rabbitmq.com/confirms.html#channel-qos-prefetch
        channel.basic_qos(1, BasicQosOptions::default()).await?;

        Ok(Self { channel })
    }

    pub async fn dispatch(&self, queue: &str, message: serde_json::Value) -> eyre::Result<()> {
        // Create the queue (if not existing already) that will receive the message.
        // Because of the default exchange below, a binding will automatically be created.
        create_queue(&self.channel, queue).await?;

        // This uses Rabbit's default exchange (https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-default),
        // which automatically binds to every declared queue.
        // Consider implementing multiple exchanges and binding strategies when expanding this library's scope.
        self.channel
            .basic_publish(
                "",
                queue,
                BasicPublishOptions::default(),
                &serde_json::to_vec(&message)?,
                // https://www.rabbitmq.com/publishers.html#message-properties
                // delivery_mode is set to Persistent (2)
                BasicProperties::default().with_delivery_mode(2),
            )
            .await?;

        Ok(())
    }
}

/// Used internally.
///
/// Creates a queue with the specified name, together with its dead letter queue.
async fn create_queue(channel: &lapin::Channel, queue_name: &str) -> eyre::Result<()> {
    if queue_name.starts_with("reserved.") {
        bail!("Invalid queue name: {}", queue_name.to_string())
    }

    let dead_letter_queue_name = create_dead_letter_queue(channel, queue_name).await?;

    let mut fields = amq_protocol_types::FieldTable::default();
    fields.insert(
        "x-dead-letter-exchange".into(),
        amq_protocol_types::AMQPValue::LongString("".into()),
    );
    fields.insert(
        "x-dead-letter-routing-key".into(),
        amq_protocol_types::AMQPValue::LongString(dead_letter_queue_name.into()),
    );

    channel
        .queue_declare(
            queue_name,
            lapin::options::QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            fields,
        )
        .await?;

    Ok(())
}

/// Used internally.
///
/// Creates the dead letter queue for the specified queue.
async fn create_dead_letter_queue(
    channel: &lapin::Channel,
    queue_name: &str,
) -> eyre::Result<String> {
    let dead_letter_queue_name = format!("reserved.dlx.{queue_name}");

    channel
        .queue_declare(
            &dead_letter_queue_name,
            lapin::options::QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            amq_protocol_types::FieldTable::default(),
        )
        .await?;

    Ok(dead_letter_queue_name)
}
