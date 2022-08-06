#![allow(dead_code)]

use crate::Nvme;
use colored_json::prelude::*;
use rpc::mayastor::{
    bdev_rpc_client::BdevRpcClient,
    mayastor_client::MayastorClient,
    CreatePoolRequest,
    CreateReplicaRequest,
    DestroyPoolRequest,
    DestroyReplicaRequest,
    Null,
    ReplicaV2,
    ShareProtocolReplica,
};
use serde::Serialize;
use std::{
    cell::RefCell,
    fmt::{Display, Formatter},
    marker::PhantomData,
    str::FromStr,
};
use tonic::transport::{Channel, Endpoint};
use url::Url;

/// TODO
pub(super) struct Pool<'a> {
    pub(super) ctx: &'a Context<'a>,
    pub(super) name: String,
    pub(super) used: u64,
    pub(super) capacity: u64,
}

impl Display for Pool<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let p = 100.0 * self.used as f64 / self.capacity as f64;
        write!(
            f,
            "<{}: {}MiB / {}MiB ({:.1}%)>",
            self.name,
            self.used / (1024 * 1024),
            self.capacity / (1024 * 1024),
            p
        )
    }
}

/// TODO
pub(super) struct Replica<'a> {
    pub(super) ctx: &'a Context<'a>,
    pub(super) name: String,
    pub(super) uuid: String,
    pub(super) pool: String,
    pub(super) uri: String,
    pub(super) shared: Option<String>,
    pub(super) size: u64,
    pub(super) thin: bool,
    pub(super) device: Option<String>,
}

impl Display for Replica<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<{}: {}MiB {}|{}{}>",
            self.name,
            self.size / (1024 * 1024),
            match self.thin {
                true => "(thin) ",
                false => "",
            },
            match self.shared {
                Some(ref s) => format!(" '{}'", s),
                None => " unshared".into(),
            },
            match self.device {
                Some(ref s) => format!(" -> {}", s),
                None => "".into(),
            }
        )
    }
}

impl<'a> Replica<'a> {
    /// TODO
    pub(super) async fn connect(&mut self) -> Result<(), String> {
        if let Some(ref nqn) = self.shared {
            match self.ctx.nvme.connect(&nqn) {
                Ok(dev) => {
                    self.device = Some(dev);
                    println!("Connected: {}", self);
                    Ok(())
                }
                Err(e) => Err(format!("Cannot connect '{}': {}", nqn, e)),
            }
        } else {
            Err(format!("Replica '{}' not shared", self.name))
        }
    }

    /// TODO
    pub(super) async fn disconnect(&mut self) -> Result<(), String> {
        if let Some(ref nqn) = self.shared {
            match self.ctx.nvme.disconnect(&nqn) {
                Ok(_) => {
                    self.device = None;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(())
        }
    }
}

/// TODO
#[allow(dead_code)]
pub(super) struct Context<'a> {
    /// TODO
    pub(super) nvme: Nvme,
    /// TODO
    ms_client: RefCell<MayastorClient<Channel>>,
    /// TODO
    bdev_client: RefCell<BdevRpcClient<Channel>>,
    /// TODO
    _a: PhantomData<&'a ()>,
}

impl<'a> Context<'a> {
    //// TODO
    pub(super) async fn new(
        addr: &str,
        ctrl_port: u16,
        nvmf_port: u16,
    ) -> Context<'a> {
        let ep = Endpoint::from_str(&format!("http://{}:{}", addr, ctrl_port))
            .unwrap();

        Self {
            nvme: Nvme::new(addr, nvmf_port),
            ms_client: RefCell::new(
                MayastorClient::<Channel>::connect(ep.clone())
                    .await
                    .unwrap(),
            ),
            bdev_client: RefCell::new(
                BdevRpcClient::<Channel>::connect(ep).await.unwrap(),
            ),
            _a: Default::default(),
        }
    }

    /// TODO
    pub(super) async fn cleanup(&self) {
        self.nvme.disconnect_all();
    }

    /// TODO
    pub(super) async fn create_pool(
        &self,
        name: &str,
        disk: &str,
    ) -> Result<(), String> {
        println!("Creating pool '{}' on '{}'...", name, disk);

        let disks = vec![disk.to_string()];

        self.ms_client
            .borrow_mut()
            .create_pool(CreatePoolRequest {
                name: name.to_string(),
                disks,
            })
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    /// TODO
    pub(super) async fn destroy_pool(&self, name: &str) -> Result<(), String> {
        println!("Destroying pool '{}'...", name);

        self.ms_client
            .borrow_mut()
            .destroy_pool(DestroyPoolRequest {
                name: name.to_string(),
            })
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    /// TODO
    pub(super) async fn list_pools(&'a self) -> Result<Vec<Pool<'a>>, String> {
        let res = self
            .ms_client
            .borrow_mut()
            .list_pools(Null {})
            .await
            .map_err(|e| e.to_string())?;

        let res = res
            .get_ref()
            .pools
            .iter()
            .map(|r| Pool {
                ctx: self,
                name: r.name.clone(),
                used: r.used,
                capacity: r.capacity,
            })
            .collect();

        Ok(res)
    }

    /// TODO
    pub(super) async fn get_pool(
        &'a self,
        name: &str,
    ) -> Result<Pool<'a>, String> {
        let res = self.list_pools().await?;
        match res.into_iter().find(|r| r.name == name) {
            Some(r) => Ok(r),
            None => Err(format!("Pool '{}' not found", name)),
        }
    }

    // /// TODO
    // pub(super) async fn list_pools(&self) -> Result<(), String> {
    //     let res = self
    //         .ms_client
    //         .borrow_mut()
    //         .list_pools(Null {})
    //         .await
    //         .map_err(|e| e.to_string())?;
    //
    //     self.print_json(res.get_ref());
    //
    //     Ok(())
    // }

    /// TODO
    pub(super) async fn list_bdevs(&self) -> Result<(), String> {
        let res = self
            .bdev_client
            .borrow_mut()
            .list(Null {})
            .await
            .map_err(|e| e.to_string())?;

        self.print_json(res.get_ref());

        Ok(())
    }

    /// TODO
    pub(super) async fn create_replica(
        &self,
        pool: &str,
        name: &str,
        size: u64,
        thin: bool,
    ) -> Result<(), String> {
        self.ms_client
            .borrow_mut()
            .create_replica(CreateReplicaRequest {
                uuid: name.to_string(),
                pool: pool.to_string(),
                size: size * 1024 * 1024,
                thin,
                share: 1,
            })
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// TODO
    pub(crate) async fn destroy_replica(
        &self,
        name: &str,
    ) -> Result<(), String> {
        self.ms_client
            .borrow_mut()
            .destroy_replica(DestroyReplicaRequest {
                uuid: name.to_string(),
            })
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// TODO
    pub(super) async fn list_replicas(
        &'a self,
    ) -> Result<Vec<Replica<'a>>, String> {
        let res = self
            .ms_client
            .borrow_mut()
            .list_replicas_v2(Null {})
            .await
            .map_err(|e| e.to_string())?;

        let res = res
            .get_ref()
            .replicas
            .iter()
            .map(|r| Replica {
                ctx: self,
                name: r.name.clone(),
                uuid: r.uuid.clone(),
                pool: r.pool.clone(),
                uri: r.uri.clone(),
                shared: Self::parse_shared_nqn(&r),
                size: r.size.into(),
                thin: r.thin,
                device: None,
            })
            .collect();

        Ok(res)
    }

    /// TODO
    pub(super) async fn get_replica(
        &'a self,
        name: &str,
    ) -> Result<Replica<'a>, String> {
        let res = self.list_replicas().await?;
        match res.into_iter().find(|r| r.name == name) {
            Some(r) => Ok(r),
            None => Err(format!("Replica '{}' not found", name)),
        }
    }

    /// TODO
    pub(super) async fn print_replicas(&self) {
        println!("Listing replicas...");
        match self.list_replicas().await {
            Ok(res) => {
                println!("{} replica(s) found", res.len());
                for r in res {
                    println!("    {}", r);
                }
            }
            Err(e) => {
                println!("Failed to list replicas: {}", e);
            }
        }
    }

    /// TODO
    fn parse_shared_nqn(r: &ReplicaV2) -> Option<String> {
        if ShareProtocolReplica::from_i32(r.share)
            == Some(ShareProtocolReplica::ReplicaNone)
        {
            return None;
        }

        if let Ok(parts) = Url::parse(&r.uri) {
            if parts.scheme() == "nvmf" {
                Some(parts.path()[1 ..].to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// TODO
    fn print_json<T>(&self, obj: &T)
    where
        T: ?Sized + Serialize,
    {
        println!(
            "{}",
            serde_json::to_string_pretty(obj)
                .unwrap()
                .to_colored_json_auto()
                .unwrap()
        );
    }
}
