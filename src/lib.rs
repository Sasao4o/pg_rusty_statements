use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

static mut PREV_EXECUTOR_START_HOOK : pg_sys::ExecutorStart_hook_type = None;
static mut PREV_EXECUTOR_END_HOOK : pg_sys::ExecutorEnd_hook_type = None;
static mut PREV_EXECUTOR_FINISH_HOOK : pg_sys::ExecutorFinish_hook_type = None;
static mut PREV_SHMEM_STARTUP_HOOK : pg_sys::shmem_startup_hook_type = None;
static mut PREV_SHMEM_REQUEST_HOOK : pg_sys::shmem_request_hook_type = None;
static mut HTAB_LOCK : *mut pg_sys::LWLock = std::ptr::null_mut();

const QUERY_STRING_MAX_LENGTH: usize = 1024;
const MAX_ENTRIES: i64 = 1000;

const IS_DEBUG : bool = true;
 
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
    if IS_DEBUG {
        pgrx::info!("hello_pg_rusty_statements called");
    }

    "Hello, pg_rusty_statements"
}


#[no_mangle]
fn _PG_init() {

    if unsafe { !pg_sys::process_shared_preload_libraries_in_progress } {
        pgrx::error!("this extension must be loaded via shared_preload_libraries because it requires shared memory.");
    }
    
    if IS_DEBUG {
    pgrx::info!("_PG_init called");
    }
    unsafe {
        pg_sys::EnableQueryId(); //this is because i depend on postgres normalization to generate query IDs

        PREV_EXECUTOR_START_HOOK = pg_sys::ExecutorStart_hook;
        PREV_EXECUTOR_FINISH_HOOK = pg_sys::ExecutorFinish_hook;
        PREV_EXECUTOR_END_HOOK = pg_sys::ExecutorEnd_hook;

        pg_sys::ExecutorStart_hook = Some(executor_start);
        pg_sys::ExecutorFinish_hook = Some(say_end);
        pg_sys::ExecutorEnd_hook = Some(executor_end);

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
    HTAB_LOCK = &mut (*locks).lock;

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


#[pg_extern]
fn get_query_stats() -> TableIterator<'static, (name!(query_string, String), name!(calls, i64), name!(total_time_ms, f64), name!(total_rows, i64))> {
    let mut stats = Vec::new();

    unsafe {
        pg_sys::LWLockAcquire(HTAB_LOCK, pg_sys::LWLockMode::LW_SHARED);

        let mut status: pg_sys::HASH_SEQ_STATUS = std::mem::zeroed();
        pg_sys::hash_seq_init(&mut status, QUERY_HTAB);

        loop {
            let entry = pg_sys::hash_seq_search(&mut status) as *mut QueryEntry;
            if entry.is_null() {
                break;
            }

            let entry = &*entry;
            let stored_len = entry.query_string_len as usize;
            let query_string = String::from_utf8_lossy(&entry.query_string[..stored_len]).into_owned();
            stats.push((query_string, entry.calls as i64, entry.total_time_ms, entry.total_rows as i64));
        }
        // hash_seq_search already terminates the scan when it returns NULL; no explicit hash_seq_term needed here.

        pg_sys::LWLockRelease(HTAB_LOCK);
    }

    TableIterator::new(stats)
}


#[pg_guard]
unsafe extern "C-unwind" fn executor_start(query_desc: *mut pg_sys::QueryDesc, eflags: i32) {
    if let Some(prev_hook) = PREV_EXECUTOR_START_HOOK {
        prev_hook(query_desc, eflags);
    } else {
        pg_sys::standard_ExecutorStart(query_desc, eflags);
    }

    // Postgres updates totaltime around ExecutorRun/Finish; allocate in es_query_cxt so it lives until ExecutorEnd.
    if (*(*query_desc).plannedstmt).queryId != 0 && (*query_desc).totaltime.is_null() {
        let old_cxt = pg_sys::MemoryContextSwitchTo((*(*query_desc).estate).es_query_cxt);
        (*query_desc).totaltime =
            pg_sys::InstrAlloc(1, pg_sys::InstrumentOption::INSTRUMENT_ALL as i32, false);
        pg_sys::MemoryContextSwitchTo(old_cxt);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn executor_end(query_desc: *mut pg_sys::QueryDesc) {
    let query_hash = (*(*query_desc).plannedstmt).queryId;
    let totaltime = (*query_desc).totaltime;

    if query_hash != 0 && !totaltime.is_null() {
        pg_sys::InstrEndLoop(totaltime);
        let elapsed_ms = (*totaltime).total * 1000.0;
        let rows = (*(*query_desc).estate).es_total_processed;
        let query_string = std::ffi::CStr::from_ptr((*query_desc).sourceText).to_bytes();

        process_query(query_hash, query_string, elapsed_ms, rows);
    }

    if let Some(prev_hook) = PREV_EXECUTOR_END_HOOK {
        prev_hook(query_desc);
    } else {
        pg_sys::standard_ExecutorEnd(query_desc);
    }
}



unsafe fn process_query(query_hash: u64, query_string: &[u8], elapsed_ms: f64, rows: u64) {
    pg_sys::LWLockAcquire(HTAB_LOCK, pg_sys::LWLockMode::LW_EXCLUSIVE);

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
    if IS_DEBUG {
        pgrx::info!(
            "query_hash: {}, calls: {}, total_time_ms: {}, total_rows: {}, query: {}",
            query_hash,
            entry.calls,
            entry.total_time_ms,
            entry.total_rows,
            stored_query
        );
    }

    pg_sys::LWLockRelease(HTAB_LOCK);
}


unsafe extern "C-unwind" fn say_end(query_desc: *mut pg_sys::QueryDesc) {
    if IS_DEBUG {
        pgrx::info!("Hello from say_end");
    }
    if let Some(prev_hook) = PREV_EXECUTOR_FINISH_HOOK {
        prev_hook(query_desc);
    } else {
        pg_sys::standard_ExecutorFinish(query_desc);
    }
}

 