use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{Debug, Display, Formatter},
};

use async_trait::async_trait;
use snafu::ResultExt;
use url::Url;

use spdk_rs::UntypedBdev;

use crate::{
    bdev::{util::uri, CreateDestroy, GetName},
    bdev_api,
    bdev_api::BdevError,
};

use super::{ArrayBdev, ArrayDeviceGroup, ArrayParams, ArraySpan};

/// TODO
#[derive(Debug)]
pub struct ArrayBdevUri {
    pub name: String,
    pub alias: String,
    pub uuid: uuid::Uuid,
    pub device_names: Vec<String>,
}

impl Display for ArrayBdevUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Array URI spec '{name}' ({uuid}); disks: <{disks}>",
            name = self.name,
            uuid = self.uuid,
            disks = self.device_names.join("; ")
        )
    }
}

/// Convert a URI to an Array "object"
impl TryFrom<&Url> for ArrayBdevUri {
    type Error = BdevError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        info!("%%%% Array URI spec '{url}' :: FROM URI ...");

        let mut parameters: HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        let uuid = uri::uuid(parameters.remove("uuid")).context(
            bdev_api::UuidParamParseFailed {
                uri: url.to_string(),
            },
        )?;

        let uuid = uuid.unwrap_or_else(|| spdk_rs::Uuid::generate().into());

        // Disks.
        let Some(disks) = parameters.remove("disks") else {
            return Err(BdevError::InvalidUri {
                uri: url.to_string(),
                message: format!("Disks not present"),
            });
        };

        let device_names = disks
            .split(";")
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        if device_names.is_empty() {
            return Err(BdevError::InvalidUri {
                uri: url.to_string(),
                message: format!("Disk list is empty"),
            });
        }

        let res = ArrayBdevUri {
            name: url.host().unwrap().to_string(),
            alias: url.to_string(),
            uuid,
            device_names,
        };

        info!("%%%% {res} :: FROM URI OK");

        Ok(res)
    }
}

impl GetName for ArrayBdevUri {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait(?Send)]
impl CreateDestroy for ArrayBdevUri {
    type Error = BdevError;

    /// Creates an Array Bdev.
    async fn create(&self) -> Result<String, Self::Error> {
        info!("%%%% {self} :: CREATING BDEV ...");

        if UntypedBdev::lookup_by_name(&self.get_name()).is_some() {
            return Err(BdevError::BdevExists {
                name: self.get_name(),
            });
        }

        //
        let devices =
            ArrayDeviceGroup::open(&self.name, &self.device_names).await?;

        //
        let array = ArraySpan::create_array(ArrayParams {
            name: self.name.clone(),
            uuid: self.uuid,
            devices,
        })
        .await?;

        //
        ArrayBdev::create(array).await?;

        if let Some(mut bdev) = UntypedBdev::lookup_by_name(&self.get_name()) {
            if !bdev.add_alias(&self.alias) {
                warn!(
                    "%%%% {self}: failed to add alias '{alias}'",
                    alias = self.alias
                );
            }

            info!("%%%% {self} :: CREATED BDEV {bname}", bname = bdev.name());

            return Ok(self.get_name());
        }

        Err(BdevError::BdevNotFound {
            name: self.get_name(),
        })
    }

    /// Destroys the given Array Bdev.
    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        info!("%%%% {name} :: DESTROYING BDEV ...", name = self.name);

        match ArrayBdev::lookup_by_name_mut(&self.name) {
            Some(mut bdev) => {
                bdev.remove_alias(&self.alias);
                bdev.destroy().await?;
                info!("%%%% {name} :: DESTROYED BDEV", name = self.name);
                Ok(())
            }
            None => Err(BdevError::BdevNotFound {
                name: self.get_name(),
            }),
        }
    }
}
