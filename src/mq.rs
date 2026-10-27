use skw_lib_http_protos::ping::RpcPayload;
use skw_lib_shared::{
    prelude::{
        APP, AppError,
        chrono::*,
        iggy::TaskDonePublisher,
        log::*,
        rabbitmq::{TaskOutcome, run_task_consumer},
        redis::*,
    },
    skw_get_back_topic_name,
};
use skw_lib_task_protos::ping::PingTask;
use skw_ping_service::{PING_TASK_QUEUE, named_queue};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let redis_conn = APP.redis.clone().connect().await?.inner();
    // let ping_done = TaskDonePublisher::new(&skw_get_task_done_notify_topic_name::<PingTask>()).await?;

    // back_topic can be uses directly to inform client that task is ready after task is completed through RabbitMq
    let ping_done = TaskDonePublisher::new(&skw_get_back_topic_name()).await?;

    // One task per queue, run concurrently. To add another queue: add its
    // entry to `queues` in config, define its own Task struct + handler fn,
    // and add one more future to try_join!.
    let ping_task_queue = named_queue(PING_TASK_QUEUE);

    tokio::try_join!(run_task_consumer(ping_task_queue, async |t| {
        handle_ping_task(t, redis_conn.clone(), &ping_done).await
    }))?;

    Ok(())
}

async fn handle_ping_task(task: PingTask, mut redis: ConnectionManager, done: &TaskDonePublisher) -> TaskOutcome {
    match rand::random::<u8>() {
        0..=200 => {
            info!("received ping task: {}", task.message);

            let count: u64 = match redis.incr("mq:count", 1).await {
                Ok(count) => count,
                Err(e) => {
                    error!("redis incr failed: {e}");
                    return TaskOutcome::Nack { requeue: true };
                }
            };

            info!(
                "received ping task: {} ack; count: {};",
                task.message, count
            );

            let ping_payload = RpcPayload::Ping {
                ts: Utc::now(),
                count,
            };

            // best effort: the task is done either way, and acking it after a failed notify
            // keeps the (non-idempotent) incr from running twice
            if let Err(e) = done.publish(&task.signature, &ping_payload, None).await {
                warn!("task done notify to {} failed: {e}", done.topic());
            }

            TaskOutcome::Ack
        }
        _ => {
            // requeued: retried by a later delivery
            info!("received ping task: {} nack;", task.message);
            TaskOutcome::Nack { requeue: true }
        }
    }
}
