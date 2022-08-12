use super::{
    compose::rpc::v1::{
        replica::{CreateReplicaRequest, ListReplicaOptions, Replica},
        RpcHandle,
        Status,
    },
    pool::PoolBuilder,
};

#[derive(Default, Clone, Debug)]
pub struct ReplicaBuilder {
    pub pool_uuid: Option<String>,
    pub name: Option<String>,
    pub uuid: Option<String>,
    pub bdev: Option<String>,
    pub size: Option<u64>,
    pub thin: bool,
    pub share: i32,
}

impl ReplicaBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_owned());
        self
    }

    pub fn with_uuid(mut self, uuid: &str) -> Self {
        self.uuid = Some(uuid.to_owned());
        self
    }

    pub fn with_pool(mut self, p: &PoolBuilder) -> Self {
        self.pool_uuid = p.uuid.clone();
        self
    }

    pub fn with_size_mb(mut self, size_mb: u64) -> Self {
        self.size = Some(size_mb * 1024 * 1024);
        self
    }

    pub fn with_thin(mut self, thin: bool) -> Self {
        self.thin = thin;
        self
    }

    pub fn bdev(&self) -> String {
        format!(
            "bdev:///{}?uuid={}",
            self.name.as_ref().unwrap(),
            self.uuid.as_ref().unwrap()
        )
    }

    pub async fn create(&self, rpc: &mut RpcHandle) -> Result<Replica, Status> {
        rpc.replica
            .create_replica(CreateReplicaRequest {
                name: self.name.as_ref().unwrap().clone(),
                uuid: self.uuid.as_ref().unwrap().clone(),
                pooluuid: self.pool_uuid.as_ref().unwrap().clone(),
                size: self.size.unwrap(),
                thin: self.thin,
                share: self.share,
            })
            .await
            .map(|r| r.into_inner())
    }
}

pub async fn list_replicas(
    rpc: &mut RpcHandle,
) -> Result<Vec<Replica>, Status> {
    rpc.replica
        .list_replicas(ListReplicaOptions {
            name: None,
            poolname: None,
        })
        .await
        .map(|r| r.into_inner().replicas)
}
