use crate::tools::run_command_args;
use std::{collections::HashSet, ffi::OsString};

/// TODO
pub(super) struct Nvme {
    /// TODO
    ip_addr: String,
    /// TODO
    port: String,
}

impl Nvme {
    /// TODO
    pub(super) fn new(ip_addr: &str, port: u16) -> Self {
        Self {
            ip_addr: String::from(ip_addr),
            port: format!("{}", port),
        }
    }

    /// TODO
    pub(super) fn print_discover(&self) {
        println!("Discovering NVMEs...");

        if let Err(e) = run_command_args(
            "sudo",
            vec![
                "nvme",
                "discover",
                "-t",
                "tcp",
                "-a",
                &self.ip_addr,
                "-s",
                &self.port,
            ],
            Some("discover"),
        ) {
            println!("Failed to discover NVMEs: {}", e);
        }
    }

    /// TODO
    pub(super) fn connect(&self, nqn: &str) -> Result<String, String> {
        println!("Connecting to NQN '{}'", nqn);

        let before: HashSet<String> = self.list_remote().into_iter().collect();

        let res = run_command_args(
            "sudo",
            vec![
                "nvme",
                "connect",
                "-t",
                "tcp",
                "-a",
                &self.ip_addr,
                "-s",
                &self.port,
                "-n",
                nqn,
            ],
            Some("conn"),
        );

        if let Err(e) = res {
            println!("Failed to connect '{}': {}", nqn, e);
            return Err(e);
        }

        let after: HashSet<String> = self.list_remote().into_iter().collect();
        let mut diff = after.difference(&before);

        let res = diff.next();

        if res.is_none() || diff.next() != None {
            return Err("Failed to determine new NVMe device".to_string());
        }

        Ok(res.unwrap().to_string())
    }

    /// TODO
    pub(super) fn disconnect(&self, nqn: &str) -> Result<(), String> {
        println!("Disconnecting NQN '{}'...", nqn);

        let res = run_command_args(
            "sudo",
            vec!["nvme", "disconnect", "-n", nqn],
            Some("disconn"),
        );

        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// TODO
    pub(super) fn list_remote(&self) -> Vec<String> {
        match run_command_args("sudo", vec!["nvme", "list"], None) {
            Ok((_, lines)) => lines
                .iter()
                .skip(2)
                .filter_map(|s| {
                    let s = s.to_str().unwrap();
                    if s.contains("DataCore NVMe controller") {
                        Some(
                            s.split_ascii_whitespace()
                                .next()
                                .unwrap()
                                .to_string(),
                        )
                    } else {
                        None
                    }
                })
                .collect(),
            Err(e) => {
                println!("Failed to discover NVMEs: {}", e);
                vec![]
            }
        }
    }

    /// TODO
    pub(super) fn print_list(&self) {
        println!("Listing all NVMEs...");

        if let Err(e) =
            run_command_args("sudo", vec!["nvme", "list"], Some("list"))
        {
            println!("Failed to list NVMEs: {}", e);
        }
    }

    /// TODO
    pub(super) fn disconnect_all(&self) {
        println!("Disconnecting all NVMEs...");

        if let Err(e) = run_command_args(
            "sudo",
            vec!["nvme", "disconnect-all"],
            Some("disconnect-all"),
        ) {
            println!("Failed to disconnect all NVMEs: {}", e);
        } else {
            println!("Disconnecting all NVMEs OK");
        }
    }
}
