use super::{
    compose::rpc::v1::{
        nexus::{
            CreateNexusRequest,
            ListNexusOptions,
            Nexus,
            PublishNexusRequest,
        },
        RpcHandle,
        Status,
    },
    replica::ReplicaBuilder,
};

#[derive(Clone, Debug)]
pub struct NexusBuilder {
    pub name: Option<String>,
    pub uuid: Option<String>,
    pub size: Option<u64>,
    pub min_cntl_id: u32,
    pub max_cntl_id: u32,
    pub resv_key: u64,
    pub preempt_key: u64,
    pub children: Option<Vec<String>>,
    pub nexus_info_key: Option<String>,
}

impl Default for NexusBuilder {
    fn default() -> Self {
        Self {
            name: None,
            uuid: None,
            size: None,
            min_cntl_id: 1,
            max_cntl_id: 1,
            resv_key: 1,
            preempt_key: 0,
            children: None,
            nexus_info_key: None,
        }
    }
}

impl NexusBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_owned());
        self.nexus_info_key = Some(name.to_owned());
        self
    }

    pub fn with_uuid(mut self, uuid: &str) -> Self {
        self.uuid = Some(uuid.to_owned());
        self
    }

    pub fn with_size_mb(mut self, size_mb: u64) -> Self {
        self.size = Some(size_mb * 1024 * 1024);
        self
    }

    pub fn with_children(mut self, bdevs: Vec<String>) -> Self {
        self.children = Some(bdevs);
        self
    }

    pub fn with_bdev(mut self, bdev: &str) -> Self {
        if self.children.is_none() {
            self.children = Some(vec![]);
        }
        self.children.as_mut().unwrap().push(bdev.to_owned());
        self
    }

    pub fn with_replica(self, r: &ReplicaBuilder) -> Self {
        self.with_bdev(&r.bdev())
    }

    pub async fn create(&self, rpc: &mut RpcHandle) -> Result<Nexus, Status> {
        rpc.nexus
            .create_nexus(CreateNexusRequest {
                name: self.name.as_ref().unwrap().clone(),
                uuid: self.uuid.as_ref().unwrap().clone(),
                size: self.size.unwrap(),
                min_cntl_id: self.min_cntl_id,
                max_cntl_id: self.max_cntl_id,
                resv_key: self.resv_key,
                preempt_key: self.resv_key,
                children: self.children.as_ref().unwrap().clone(),
                nexus_info_key: self.nexus_info_key.as_ref().unwrap().clone(),
            })
            .await
            .map(|r| r.into_inner().nexus.unwrap())
    }

    pub async fn publish(&self, rpc: &mut RpcHandle) -> Result<Nexus, Status> {
        rpc.nexus
            .publish_nexus(PublishNexusRequest {
                uuid: self.uuid.as_ref().unwrap().clone(),
                key: String::new(),
                share: 1,
            })
            .await
            .map(|r| r.into_inner().nexus.unwrap())
    }
}

pub async fn list_nexuses(rpc: &mut RpcHandle) -> Result<Vec<Nexus>, Status> {
    rpc.nexus
        .list_nexus(ListNexusOptions {
            name: None,
        })
        .await
        .map(|r| r.into_inner().nexus_list)
}
