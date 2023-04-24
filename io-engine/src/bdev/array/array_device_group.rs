use std::fmt::{Display, Formatter};

use super::{ArrayDevice, ArrayError};

/// TODO
pub struct ArrayDeviceGroup {
    name: String,
    devices: Vec<ArrayDevice>,
}

impl Display for ArrayDeviceGroup {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Array dev group: '{name}': <{devs}>",
            name = self.name,
            devs = self.device_names().join("; "),
        )
    }
}

impl Drop for ArrayDeviceGroup {
    fn drop(&mut self) {
        assert!(
            self.devices.is_empty(),
            "Array device group must be closed before dropping"
        )
    }
}

impl ArrayDeviceGroup {
    /// TODO
    pub async fn open(
        group_name: &str,
        device_names: &[String],
    ) -> Result<ArrayDeviceGroup, ArrayError> {
        info!("%%%% Array dev group: {group_name} :: OPENING {device_names:?} ...");

        if device_names.is_empty() {
            return Err(ArrayError::NotEnoughDevices {
                array_name: group_name.to_owned(),
            });
        }

        let mut devices = Vec::new();

        for name in device_names {
            match ArrayDevice::open(name).await {
                Ok(d) => {
                    devices.push(d);
                }
                Err(e) => {
                    error!("%%%% failed to open SPDK device '{name}': {e}");
                    Self::close_devices(devices.into_iter()).await;
                    return Err(e);
                }
            }
        }

        let res = Self {
            name: group_name.to_owned(),
            devices,
        };

        info!("%%%% {res} :: OPEN");

        Ok(res)
    }

    /// TODO
    pub async fn close(&mut self) {
        let s = self.to_string();
        info!("%%%% {s} :: CLOSING ...");
        Self::close_devices(self.devices.drain(..)).await;
        info!("%%%% {s} :: CLOSED");
    }

    /// TODO
    async fn close_devices(devices: impl Iterator<Item = ArrayDevice>) {
        for d in devices {
            let name = format!("{}", d.name());
            if let Err(e) = d.close().await {
                error!("%%%% failed to close array disk '{name}': {e}");
            }
        }
    }

    /// TODO
    pub fn name(&self) -> &str {
        &self.name
    }

    /// TODO
    pub fn device_names(&self) -> Vec<String> {
        self.devices.iter().map(|d| d.name().to_owned()).collect()
    }

    /// TODO
    pub fn iter(&self) -> impl Iterator<Item = &ArrayDevice> {
        self.devices.iter()
    }
}
