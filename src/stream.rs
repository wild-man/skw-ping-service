use skw_lib_http_protos::{
    JsonRpcServiceRequest,
    ping::{RpcPayload, open::RpcMethod},
};
use skw_lib_shared::{AppError, prelude::iggy::run_service_consumers_all_topic_types};

#[allow(unused)]
use skw_lib_shared::prelude::{
    APP,
    chrono::*,
    consts::SERVICE_TOPIC_TYPES_PATH,
    iggy::{RpcRequestMeta, TopicType, run_service_consumer},
    jsonrpc::ServiceHttpError,
    log::*,
    redis::{AsyncCommands, ConnectionManager},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let redis = APP.redis.clone().connect().await;
    let redis_conn = redis.inner().expect("bad redis connection");

    run_service_consumers_all_topic_types::<JsonRpcServiceRequest<RpcMethod>, _, _, _>(
        move |rpc_method, meta| handle_open_message(rpc_method, meta, redis_conn.clone()),
        None,
        Some(100), //@todo maybe need to set per topic_type
    )
    .await?;

    Ok(())
}

#[allow(unused)]
async fn handle_open_message(rpc_method: RpcMethod, _meta: RpcRequestMeta, mut redis: ConnectionManager) -> Result<RpcPayload, AppError> {
    debug!("{:?}", rpc_method);
    debug!("signature:{:?}", _meta);

    match rpc_method {
        RpcMethod::Public {} => Ok(RpcPayload::Public {}),
        RpcMethod::PingOpen {} => {
            let count: u64 = redis.incr("ping:count", 1).await.map_err(|e| {
                error!("redis incr failed: {e}");
                ServiceHttpError::InternalServerError
            })?;

            Ok(RpcPayload::Ping {
                ts: Utc::now(),
                count,
            })
        }
        _ => Err(ServiceHttpError::BadRequest.into()),
    }
}
/*
RpcMethod::PingSigned {} => {
    let count: u64 = redis.incr("ping:count", 1).await.map_err(|e| {
        error!("redis incr failed: {e}");
        ServiceHttpError::InternalServerError
    })?;

    Ok(RpcPayload::Ping {
        ts: Utc::now(),
        count,
    })
}

 */
