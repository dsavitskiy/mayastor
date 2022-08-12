use io_engine::constants::NVME_CONTROLLER_MODEL_ID;
use std::process::{Command, ExitStatus};

/// Connects an NVMe device upon creation and disconnects when dropped.
pub struct NmveConnectGuard {
    nqn: String,
}

impl NmveConnectGuard {
    pub fn new(target_addr: &str, nqn: &str) -> Self {
        nvme_connect(target_addr, nqn, true);

        Self {
            nqn: nqn.to_string(),
        }
    }
}

impl Drop for NmveConnectGuard {
    fn drop(&mut self) {
        assert!(!self.nqn.is_empty());

        nvme_disconnect_nqn(&self.nqn);
        self.nqn.clear();
    }
}

pub fn nvme_discover(
    target_addr: &str,
) -> ExitStatus {
    let status = Command::new("nvme")
        .args(&["discover"])
        .args(&["-t", "tcp"])
        .args(&["-a", target_addr])
        .args(&["-s", "8420"])
        .status()
        .unwrap();

    if !status.success() {
        let msg = format!(
            "failed to discover at {}: {}",
            target_addr, status,
        );
        panic!("{}", msg);
    }

    status
}

pub fn nvme_connect(
    target_addr: &str,
    nqn: &str,
    must_succeed: bool,
) -> ExitStatus {
    let status = Command::new("nvme")
        .args(&["connect"])
        .args(&["-t", "tcp"])
        .args(&["-a", target_addr])
        .args(&["-s", "8420"])
        .args(&["-n", nqn])
        .status()
        .unwrap();

    if !status.success() {
        let msg = format!(
            "failed to connect to {}, nqn {}: {}",
            target_addr, nqn, status,
        );
        if must_succeed {
            panic!("{}", msg);
        } else {
            eprintln!("{}", msg);
        }
    } else {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    status
}

pub fn nvme_disconnect_nqn(nqn: &str) {
    let output_dis = Command::new("nvme")
        .args(&["disconnect"])
        .args(&["-n", nqn])
        .output()
        .unwrap();
    assert!(
        output_dis.status.success(),
        "failed to disconnect from {}: {}",
        nqn,
        output_dis.status
    );
}

pub fn list_mayastor_nvme_devices() -> Vec<libnvme_rs::NvmeDevice> {
    libnvme_rs::NvmeTarget::list()
        .into_iter()
        .filter(|dev| dev.model.contains(NVME_CONTROLLER_MODEL_ID))
        .collect()
}

pub fn get_nvme_resv_report(nvme_dev: &str) -> serde_json::Value {
    let output_resv = Command::new("nvme")
        .args(&["resv-report"])
        .args(&[nvme_dev])
        .args(&["-c", "1"])
        .args(&["-o", "json"])
        .output()
        .unwrap();
    assert!(
        output_resv.status.success(),
        "failed to get reservation report from {}: {}",
        nvme_dev,
        output_resv.status
    );
    let resv_rep = String::from_utf8(output_resv.stdout).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&resv_rep).expect("JSON was not well-formatted");
    v
}
