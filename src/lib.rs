extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;
use libduckdb_sys as ffi;
extern crate libc;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libc::c_char;
use std::{
    error::Error,
    ffi::{CStr, CString},
};

#[repr(C)]
struct SASBindData {
    file_path: *mut c_char,
}

impl Free for SASBindData {
    fn free(&mut self) {
        unsafe {
            if !self.file_path.is_null() {
                drop(CString::from_raw(self.file_path));
            }
        }
    }
}

#[repr(C)]
struct SASInitData {
    done: bool,
}

struct SASVTab;

impl Free for SASInitData {}

impl VTab for SASVTab {
    type InitData = SASInitData;
    type BindData = SASBindData;

    unsafe fn bind(bind: &BindInfo, data: *mut SASBindData) -> Result<(), Box<dyn Error>> {
        bind.add_result_column("data", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let param = bind.get_parameter(0).to_string();
        (*data).file_path = CString::new(param)?.into_raw();
        Ok(())
    }

    unsafe fn init(_: &InitInfo, data: *mut SASInitData) -> Result<(), Box<dyn Error>> {
        (*data).done = false;
        Ok(())
    }

    unsafe fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data::<SASInitData>();
        let bind_data = func.get_bind_data::<SASBindData>();

        if (*init_data).done {
            output.set_len(0);
        } else {
            (*init_data).done = true;
            let file_path = CStr::from_ptr((*bind_data).file_path).to_string_lossy();

            // Simule la lecture des donn�es avec readstat
            let result = format!("Lecture du fichier SAS : {}", file_path);
            let vector = output.flat_vector(0);
            vector.insert(0, result.as_str()); // Ins�rer une r�f�rence de cha�ne
            output.set_len(1);
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");

#[duckdb_entrypoint_c_api(ext_name = "readstat_duckdb", min_duckdb_version = "v0.0.1")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<SASVTab>(EXTENSION_NAME)?;
    Ok(())
}
