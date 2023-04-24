use super::ArrayBdevIoCtx;

use spdk_rs::{
    BdevModule,
    BdevModuleBuild,
    WithModuleFini,
    WithModuleGetCtxSize,
    WithModuleInit,
};

/// Name for Array Bdev module name.
pub const ARRAY_BDEV_MODULE_NAME: &str = "ARRAY_MODULE";

/// TODO
pub static ARRAY_BDEV_PRODUCT_ID: &str = "Array Driver v0.0.1";

/// TODO
#[derive(Debug)]
pub struct ArrayBdevModule {}

impl ArrayBdevModule {
    /// Returns Array Bdev module instance.
    /// Panics if the Array module was not registered.
    #[allow(dead_code)]
    pub fn current() -> BdevModule {
        match BdevModule::find_by_name(ARRAY_BDEV_MODULE_NAME) {
            Ok(m) => m,
            Err(err) => panic!("{}", err),
        }
    }
}

impl WithModuleInit for ArrayBdevModule {
    fn module_init() -> i32 {
        info!("Initializing Array Bdev Module");
        0
    }
}

impl WithModuleFini for ArrayBdevModule {
    fn module_fini() {
        info!("Unloading Array Bdev Module");
    }
}

impl WithModuleGetCtxSize for ArrayBdevModule {
    fn ctx_size() -> i32 {
        std::mem::size_of::<ArrayBdevIoCtx>() as i32
    }
}

impl BdevModuleBuild for ArrayBdevModule {}

pub(crate) fn register_module() {
    ArrayBdevModule::builder(ARRAY_BDEV_MODULE_NAME)
        .with_module_init()
        .with_module_fini()
        .with_module_ctx_size()
        .register();
}
