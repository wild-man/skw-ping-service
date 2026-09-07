use skw_lib_http_protos::{
    JsonRpcServiceRequest,
    ping::{RpcMethod, RpcPayload},
};
use skw_lib_shared::prelude::{
    APP,
    chrono::*,
    iggy::{RpcRequestMeta, run_service_consumer},
    jsonrpc::ServiceHttpError,
    log::*,
    redis::{AsyncCommands, ConnectionManager},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let redis = APP.redis.clone().connect().await;
    let redis_conn = redis.inner().expect("bad redis connection");

    run_service_consumer::<JsonRpcServiceRequest<RpcMethod>, _, _, _>(
        move |rpc_method, meta| handle_message(rpc_method, meta, redis_conn.clone()),
        None,
        Some(100),
    )
    .await?;
    Ok(())
}

async fn handle_message(rpc_method: RpcMethod, _meta: RpcRequestMeta, mut redis: ConnectionManager) -> Result<RpcPayload, ServiceHttpError> {
    debug!("{:?}", rpc_method);
    debug!("signature:{:?}", _meta);
    match rpc_method {
        RpcMethod::Public {} => Ok(RpcPayload::Public {}),
        RpcMethod::Ping {} => {
            let count: u64 = redis.incr("ping:count", 1).await.map_err(|e| {
                error!("redis incr failed: {e}");
                ServiceHttpError::InternalServerError
            })?;

            Ok(RpcPayload::Ping {
                ts: Utc::now(),
                count,
            })
        }
    }
}
