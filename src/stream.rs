use std::time::Duration;

use skw_lib_http_protos::{
    JsonRpcServiceRequest,
    ping::{RpcPayload, open::RpcMethod as RpcMethodOpen, signed::RpcMethod as RpcMethodSigned},
};

use skw_lib_shared::prelude::{
    APP, AppError,
    chrono::*,
    iggy::{RpcRequestMeta, SkwTopicType, get_config_topic_types, run_service_consumer, run_service_consumers_all_topic_types},
    jsonrpc::SkwServiceHttpError::BadRequest,
    rabbitmq::RabbitMqPublisher,
    redis::{AsyncCommands, ConnectionManager},
};
use skw_lib_task_protos::ping::PingTask;
use skw_ping_service::{PING_TASK_QUEUE, named_queue};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let redis = APP.redis.clone().connect().await;
    let redis_conn = redis.inner().expect("bad redis connection");

    let topic_types = get_config_topic_types()?;

    if topic_types
        .iter()
        .all(|t| t == &SkwTopicType::Open || t == &SkwTopicType::Public)
    {
        run_service_consumers_all_topic_types::<JsonRpcServiceRequest<RpcMethodOpen>, _, _, _>(
            move |rpc_method, meta| handle_open_message(rpc_method, meta, redis_conn.clone()),
            None,
            Some(100), //@todo maybe need to set per topic_type
        )
        .await?;
    } else {
        // only TopicType::Signed can be in topics
        let tasks = RabbitMqPublisher::new(named_queue(PING_TASK_QUEUE)).await?;

        run_service_consumer::<JsonRpcServiceRequest<RpcMethodSigned>, _, _, _>(
            move |rpc_method, meta| handle_signed_message(rpc_method, meta, redis_conn.clone(), tasks.clone()),
            None,
            Some(100),
            SkwTopicType::Signed,
        )
        .await?;
    }

    Ok(())
}

async fn handle_open_message(rpc_method: RpcMethodOpen, _meta: RpcRequestMeta, mut redis: ConnectionManager) -> Result<RpcPayload, AppError> {
    // debug!("{:?}", rpc_method);
    // debug!("signature:{:?}", meta);

    match rpc_method {
        RpcMethodOpen::Public {} => Ok(RpcPayload::Public {}),
        RpcMethodOpen::PingOpen {} => Ok(RpcPayload::Ping {
            ts: Utc::now(),
            count: redis.incr("ping:count", 1).await?,
        }),
    }
}

/// Queues a PingTask for ping-mq-service; the request is answered with a provisional reply only.
async fn enqueue_ping(tasks: &RabbitMqPublisher, meta: &RpcRequestMeta) -> Result<(), AppError> {
    let api_key = meta
        .api_key
        .as_ref()
        .ok_or_else(|| AppError::Service(BadRequest))?;

    let task = PingTask {
        message: format!("path: {}; ", meta.http_path.clone()),
        signature: meta.signature.clone(),
        api_key: (*api_key)
            .clone()
            .try_into()
            .map_err(|_| AppError::Service(BadRequest))?,
    };
    tasks.publish(&task).await?;
    Ok(())
}

async fn handle_signed_message(
    rpc_method: RpcMethodSigned,
    meta: RpcRequestMeta,
    mut redis: ConnectionManager,
    tasks: RabbitMqPublisher,
) -> Result<RpcPayload, AppError> {
    // debug!("{:?}", rpc_method);
    // debug!("signature:{:?}", _meta);

    match rpc_method {
        RpcMethodSigned::PingSigned {} => Ok(RpcPayload::Ping {
            ts: Utc::now(),
            count: redis.incr("ping:count", 1).await?,
        }),
        RpcMethodSigned::PingCreated {} => {
            enqueue_ping(&tasks, &meta).await?;
            Ok(RpcPayload::PingCreated {})
        }
        RpcMethodSigned::PingAccepted {} => {
            enqueue_ping(&tasks, &meta).await?;
            // wait to test that client will get response from rabbitMq consumer
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(RpcPayload::PingAccepted {})
        }
    }
}
