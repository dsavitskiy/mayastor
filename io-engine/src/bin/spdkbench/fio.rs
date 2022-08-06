use crate::run_command_args;
use std::ffi::OsString;

/// TODO
#[derive(Debug)]
pub enum FioJobMode {
    SeqRead,
    SeqWrite,
    SeqRw,
    RandRead,
    RandWrite,
    RandRw,
}

impl ToString for FioJobMode {
    fn to_string(&self) -> String {
        match self {
            FioJobMode::SeqRead => "read",
            FioJobMode::SeqWrite => "write",
            FioJobMode::SeqRw => "rw",
            FioJobMode::RandRead => "randread",
            FioJobMode::RandWrite => "randwrite",
            FioJobMode::RandRw => "randrw",
        }
        .to_string()
    }
}

/// TODO
/// --time_based
/// --runtime=120
#[derive(Debug)]
pub(super) struct FioJob {
    pub name: String,
    pub description: String,
    pub size: u64,
    pub filename: String,
    pub ioengine: String,
    pub blocksize: u64,
    pub mode: FioJobMode,
    pub iodepth: u16,
    pub numjobs: u16,
    pub runtime: u32,
    pub zeros: bool,
}

impl FioJob {
    /// TODO
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            description: Default::default(),
            size: 128,
            filename: Default::default(),
            ioengine: "libaio".to_string(),
            blocksize: 4 * 1024,
            mode: FioJobMode::SeqRead,
            iodepth: 1,
            numjobs: 1,
            runtime: 60,
            zeros: false,
        }
    }

    /// TODO
    pub fn args(&self) -> Vec<OsString> {
        let mut args = vec![
            format!("--name={}", self.name),
            format!("--filename={}", self.filename),
            format!("--ioengine={}", self.ioengine),
            format!("--direct=1",),
            format!("--size={}", self.size * 1024 * 1024),
            format!("--bs={}", self.blocksize),
            format!("--rw={}", self.mode.to_string()),
            format!("--iodepth={}", self.iodepth),
            format!("--numjobs={}", self.numjobs),
        ];

        if self.runtime > 0 {
            args.push(format!("--time_based"));
            args.push(format!("--runtime={}", self.runtime));
        }

        if self.zeros {
            args.push(format!("--zero_buffers"));
        }

        args.into_iter().map(|s| OsString::from(s)).collect()
    }
}

/// TODO
pub(super) struct Fio {
    /// TODO
    desc: String,
    /// TODO
    jobs: Vec<FioJob>,
}

impl Fio {
    /// TODO
    pub(super) fn new(desc: &str, jobs: Vec<FioJob>) -> Self {
        Self {
            desc: desc.to_string(),
            jobs,
        }
    }

    /// TODO
    pub(super) async fn run(&self) {
        let args =
            self.jobs
                .iter()
                .fold(vec![OsString::from("fio")], |mut acc, j| {
                    acc.append(&mut j.args());
                    acc
                });

        println!("Running fio with args:");
        println!(
            "{}",
            args.iter()
                .map(|s| s.to_str().unwrap().to_string())
                .collect::<Vec<String>>()
                .join(" ")
        );
        run_command_args("sudo", args, Some(&self.desc));
    }
}
