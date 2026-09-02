use skw_lib_http_protos::{
    JsonRpcServiceRequest,
    ping::{RpcMethod, RpcPayload},
};
use skw_lib_shared::prelude::{
    chrono::*,
    iggy::{RpcRequestMeta, run_service_consumer},
    jsonrpc::ServiceHttpError,
    log::*,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run_service_consumer::<JsonRpcServiceRequest<RpcMethod>, _, _, _>(handle_message, None, Some(100)).await?;
    Ok(())
}

async fn handle_message(rpc_method: RpcMethod, _meta: RpcRequestMeta) -> Result<RpcPayload, ServiceHttpError> {
    debug!("{:?}", rpc_method);
    debug!("signature:{:?}", _meta);
    match rpc_method {
        RpcMethod::Public {} => Ok(RpcPayload::Public {}),
        RpcMethod::Ping {} => Ok(RpcPayload::Ping { ts: Utc::now() }),
    }
}
