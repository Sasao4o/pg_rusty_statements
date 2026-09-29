use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

static mut PREV_EXECUTOR_RUN_HOOK : pg_sys::ExecutorRun_hook_type = None;
static mut PREV_EXECUTOR_FINISH_HOOK : pg_sys::ExecutorFinish_hook_type = None;
static mut PREV_SHMEM_STARTUP_HOOK : pg_sys::shmem_startup_hook_type = None;
static mut PREV_SHMEM_REQUEST_HOOK : pg_sys::shmem_request_hook_type = None;
static mut MY_LOCK : *mut pg_sys::LWLock = std::ptr::null_mut();

const QUERY_STRING_MAX_LENGTH: usize = 1024;
const MAX_ENTRIES: i64 = 1000;


 
#[repr(C)]
struct QueryHashKey {
    query_hash: u64
}

#[repr(C)]
struct QueryEntry {
    key: QueryHashKey,
    calls : u64,
    total_time_ms : f64,
    total_rows : u64,
    query_string_len: u16,
    query_string: [u8; QUERY_STRING_MAX_LENGTH]
}

static mut QUERY_HTAB : *mut pg_sys::HTAB = std::ptr::null_mut();

#[pg_extern]
fn hello_pg_rusty_statements() -> &'static str {
    "Hello, pg_rusty_statements"
}


#[no_mangle]
fn _PG_init() {

    if unsafe { !pg_sys::process_shared_preload_libraries_in_progress } {
        pgrx::error!("this extension must be loaded via shared_preload_libraries because it requires shared memory.");
    }
 
    pgrx::info!("_PG_init called");
    unsafe {
        pg_sys::EnableQueryId(); //this is because i depend on postgres normalization to generate query IDs

        PREV_EXECUTOR_RUN_HOOK = pg_sys::ExecutorRun_hook;
        PREV_EXECUTOR_FINISH_HOOK = pg_sys::ExecutorFinish_hook;

        pg_sys::ExecutorRun_hook = Some(execute_run);
        pg_sys::ExecutorFinish_hook = Some(say_end);

    }
    
    
    unsafe {
        PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
        pg_sys::shmem_request_hook = Some(shmem_request);

        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(shmem_startup);
    }
    
 }

unsafe extern "C-unwind" fn shmem_request() {
    if let Some(prev_hook) = PREV_SHMEM_REQUEST_HOOK {
        prev_hook();
    }
    pg_sys::RequestAddinShmemSpace(
        pg_sys::hash_estimate_size(MAX_ENTRIES, std::mem::size_of::<QueryEntry>())
    );
    pg_sys::RequestNamedLWLockTranche(c"my_extension_tranche".as_ptr(), 1);
}

unsafe extern "C-unwind" fn shmem_startup() {
    if let Some(prev_hook) = PREV_SHMEM_STARTUP_HOOK {
        prev_hook();
    }

    let locks = pg_sys::GetNamedLWLockTranche(c"my_extension_tranche".as_ptr());
    MY_LOCK = &mut (*locks).lock;

    let mut info: pg_sys::HASHCTL = std::mem::zeroed();
    info.keysize = std::mem::size_of::<QueryHashKey>() as _;
    info.entrysize = std::mem::size_of::<QueryEntry>() as _;

    QUERY_HTAB = pg_sys::ShmemInitHash(
        c"pg_rusty_statements query hash".as_ptr(),
        MAX_ENTRIES,
        MAX_ENTRIES,
        &mut info,
        (pg_sys::HASH_ELEM | pg_sys::HASH_BLOBS) as i32,
    );
}

fn set_query_string(dest: &mut [u8; QUERY_STRING_MAX_LENGTH], src: &[u8]) -> u16 {
    let len = src.len().min(dest.len());
    dest[..len].copy_from_slice(&src[..len]);
    dest[len..].fill(0);
    len as u16
}

unsafe fn process_query(query_hash: u64, query_string: &[u8], elapsed_ms: f64, rows: u64) {
    pg_sys::LWLockAcquire(MY_LOCK, pg_sys::LWLockMode::LW_EXCLUSIVE);

    let key = QueryHashKey { query_hash };
    let mut found: bool = false;
    let entry = pg_sys::hash_search(
        QUERY_HTAB,
        &key as *const QueryHashKey as *const std::ffi::c_void,
        pg_sys::HASHACTION::HASH_ENTER,
        &mut found,
    ) as *mut QueryEntry;
    let entry = &mut *entry;

    if !found {
        entry.calls = 0;
        entry.total_time_ms = 0.0;
        entry.total_rows = 0;
        entry.query_string_len = 0;
    }

    entry.query_string_len = set_query_string(&mut entry.query_string, query_string);

    entry.calls += 1;
    entry.total_time_ms += elapsed_ms;
    entry.total_rows += rows;

    let stored_len = entry.query_string_len as usize;
    let stored_query = String::from_utf8_lossy(&entry.query_string[..stored_len]);

    pgrx::info!(
        "query_hash: {}, calls: {}, total_time_ms: {}, total_rows: {}, query: {}",
        query_hash,
        entry.calls,
        entry.total_time_ms,
        entry.total_rows,
        stored_query
    );

    pg_sys::LWLockRelease(MY_LOCK);
}

unsafe fn hash_query_string(query_string: &[u8]) -> u64 {
    // simple FNV-1a hash; good enough as a shared-memory hash table key
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in query_string {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[pg_guard]
unsafe extern "C-unwind" fn execute_run( query_desc: *mut pg_sys::QueryDesc,
    direction: pg_sys::ScanDirection::Type,
    count: pg_sys::uint64, execute_once: bool) {
    let start = std::time::Instant::now();

    if let Some(prev_hook) = PREV_EXECUTOR_RUN_HOOK {
        prev_hook(query_desc, direction, count, execute_once);
    } else {
        pg_sys::standard_ExecutorRun(query_desc, direction, count, execute_once);
    }

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows = (*(*query_desc).estate).es_processed as u64;

    let query_string = std::ffi::CStr::from_ptr((*query_desc).sourceText).to_bytes();
    let query_hash = (*(*query_desc).plannedstmt).queryId; 


    process_query(query_hash, query_string, elapsed_ms, rows);
}


unsafe extern "C-unwind" fn say_end(query_desc: *mut pg_sys::QueryDesc) {
    pgrx::info!("Hello from say_end");
    if let Some(prev_hook) = PREV_EXECUTOR_FINISH_HOOK {
        prev_hook(query_desc);
    } else {
        pg_sys::standard_ExecutorFinish(query_desc);
    }
}


#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pg_rusty_statements() {
        assert_eq!("Hello, pg_rusty_statements", crate::hello_pg_rusty_statements());
    }

}


#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use pgrx::prelude::*;
    use pgrx_bench::{Bencher, black_box};

    #[pg_bench]
    fn bench_hello_pg_rusty_statements(b: &mut Bencher) {
        b.iter(|| {
            black_box(crate::hello_pg_rusty_statements());
        });
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
