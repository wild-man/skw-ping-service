use skw_lib_http_protos::{
    JsonRpcServiceRequest,
    ping::{RpcPayload, open::RpcMethod as RpcMethodOpen, signed::RpcMethod as RpcMethodSigned},
};

use skw_lib_shared::prelude::{
    APP, AppError,
    chrono::*,
    iggy::{RpcRequestMeta, SkwTopicType, get_config_topic_types, run_service_consumer, run_service_consumers_all_topic_types},
    redis::{AsyncCommands, ConnectionManager},
};

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
        run_service_consumer::<JsonRpcServiceRequest<RpcMethodSigned>, _, _, _>(
            move |rpc_method, meta| handle_signed_message(rpc_method, meta, redis_conn.clone()),
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
    // debug!("signature:{:?}", _meta);

    match rpc_method {
        RpcMethodOpen::Public {} => Ok(RpcPayload::Public {}),
        RpcMethodOpen::PingOpen {} => Ok(RpcPayload::Ping {
            ts: Utc::now(),
            count: redis.incr("ping:count", 1).await?,
        }),
    }
}

async fn handle_signed_message(rpc_method: RpcMethodSigned, _meta: RpcRequestMeta, mut redis: ConnectionManager) -> Result<RpcPayload, AppError> {
    // debug!("{:?}", rpc_method);
    // debug!("signature:{:?}", _meta);

    match rpc_method {
        RpcMethodSigned::PingSigned {} => Ok(RpcPayload::Ping {
            ts: Utc::now(),
            count: redis.incr("ping:count", 1).await?,
        }),
    }
}
