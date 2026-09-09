use serde_derive::Deserialize;
use skw_lib_shared::prelude::{
    APP,
    consts::RABBITMQ_QUEUES_PATH,
    log::*,
    rabbitmq::{QueueConfig, TaskOutcome, run_task_consumer},
    redis::*,
};
use skw_lib_task_protos::ping::PingTask;

#[derive(Debug, Deserialize)]
struct QueueSettings {
    name: String,
    prefetch: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let queues: Vec<QueueSettings> =
        serde_json::from_value(APP.config.expect_value_cloned(RABBITMQ_QUEUES_PATH)).expect("invalid /rabbitmq/queues config");

    let redis = APP.redis.clone().connect().await;
    let redis_conn = redis.inner().expect("bad redis connection");

    // One task per queue, run concurrently. To add another queue: add its
    // entry to `queues` in config, define its own Task struct + handler fn,
    // and add one more future to try_join!.
    let ping_task_queue = named_queue(&queues, "ping-task");

    tokio::try_join!(run_task_consumer(ping_task_queue, async |t| {
        handle_ping_task(t, redis_conn.clone()).await
    }))?;

    Ok(())
}

fn named_queue(queues: &[QueueSettings], name: &str) -> QueueConfig {
    let settings = queues
        .iter()
        .find(|q| q.name == name)
        .unwrap_or_else(|| panic!("queue '{name}' missing from /rabbitmq/queues config"));

    QueueConfig::new(settings.name.as_str()).prefetch(settings.prefetch)
}

async fn handle_ping_task(task: PingTask, mut redis: ConnectionManager) -> TaskOutcome {
    match rand::random::<u8>() {
        0..200 => {
            info!("received ping task: {}", task.message);
            let _count: u64 = redis
                .incr("mq:count", 1)
                .await
                .map_err(|e| {
                    error!("redis incr failed: {e}");
                })
                .unwrap();

            TaskOutcome::Ack
        }
        _ => {
            info!("received ping task: {} nack:", task.message);
            TaskOutcome::Nack { requeue: true }
        }
    }
}
