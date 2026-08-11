use libwing::WingNodeData;
use rustler::{ ResourceArc,NifTaggedEnum};
use rustler::{Env, Term, NifResult, Encoder, OwnedEnv, LocalPid};

use libwing::{WingConsole, WingResponse,WingNodeDef,DiscoveryInfo};

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

rustler::atoms! {
    channel,
    mix,
    aux,
    main,
    matrix,
    wing_reader_data,
    wing_reader_error
}

struct ExWing { pub wing: Mutex<WingConsole> }

type WingArc = ResourceArc<ExWing>;

/// Handle for the per-console reader thread. Holds the stop flag the thread
/// polls and a console clone so stop_reader_thread() can shut the socket
/// down, which wakes a read() blocked inside the thread.
struct ReaderHandle {
    stop: AtomicBool,
    wing: WingConsole,
}

type ReaderArc = ResourceArc<ReaderHandle>;

#[derive(NifTaggedEnum)]
pub enum WingResponseSimple {
    RequestEnd,
    NodeDef(WingNodeDef),
    NodeData(i32, WingNodeData),
    NodeDataSimple(String, WingNodeData),
}

fn on_load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(ExWing, env);
    let _ = rustler::resource!(ReaderHandle, env);
    true
}

// Connecting does blocking network I/O (TCP connect, possibly a UDP
// discovery scan) that can take seconds — never run it on a normal
// scheduler. Returns {:ok, ref} | {:error, reason} instead of panicking
// the NIF on an unreachable console.
#[rustler::nif(schedule = "DirtyIo")]
fn connect() -> Result<WingArc, String> {
    match WingConsole::connect(None) {
        Ok(wing) => Ok(ResourceArc::new(ExWing { wing: Mutex::new(wing) })),
        Err(e) => Err(format!("{:?}", e)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn connect_with_host(host: Option<String>) -> Result<WingArc, String> {
    match WingConsole::connect(host.as_deref()) {
        Ok(wing) => Ok(ResourceArc::new(ExWing { wing: Mutex::new(wing) })),
        Err(e) => Err(format!("{:?}", e)),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn read_simple(wing_arc: WingArc) -> WingResponseSimple {
    let mut wing = wing_arc.wing.lock().unwrap();
    let wing: &mut WingConsole = &mut *wing;

    loop {
        if let Ok(WingResponse::NodeData(id, data)) =  wing.read() {
            match WingConsole::id_to_defs(id) {

                Some(defs) if defs.is_empty() => return WingResponseSimple::NodeData(id, data),
                Some(defs) if defs.len() == 1 => {
                    let longname = format!("{}", defs[0].0);
                    return WingResponseSimple::NodeDataSimple(longname, data);
                }
                Some(defs) if (defs.len() > 1) => continue,
                Some(_) => continue,
                None => continue,
            }
        }
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn read(wing_arc: WingArc) -> WingResponse {
    let mut wing = wing_arc.wing.lock().unwrap();
    let wing: &mut WingConsole = &mut *wing;

    loop {
        if let Ok(response) = wing.read() {
            return response;
        }
    }
}
#[rustler::nif(schedule = "DirtyCpu")]
fn scan() -> Vec<DiscoveryInfo> {
    match WingConsole::scan(false) {
        Ok(discovery_info) => discovery_info,
        Err(_) => Vec::new(),
    }
}

/// Start the single reader thread for a console connection.
///
/// The thread shares the console's TCP connection (reads lock rsock, writes
/// lock wsock — they don't contend), so it does NOT open a connection of its
/// own. Every NodeData response is forwarded to `pid` as
/// {:wing_reader_data, id, float}; subscription filtering happens in Elixir.
/// On a read error the thread sends {:wing_reader_error, reason} (unless it
/// was stopped deliberately) and exits.
///
/// Returns a handle for stop_reader_thread/1. Without calling that, the
/// thread lives as long as the connection does — it also exits when the
/// owning pid dies and a send fails, closing the socket behind it.
#[rustler::nif]
fn start_reader_thread(wing_arc: WingArc, pid_term: Term) -> NifResult<ReaderArc> {
    let pid: LocalPid = pid_term.decode()?;
    let wing = wing_arc.wing.lock().unwrap().clone();

    let handle = ResourceArc::new(ReaderHandle {
        stop: AtomicBool::new(false),
        wing,
    });

    let thread_handle = handle.clone();
    thread::spawn(move || {
        let mut wing = thread_handle.wing.clone();

        loop {
            if thread_handle.stop.load(Ordering::Acquire) {
                break;
            }

            match wing.read() {
                Ok(WingResponse::NodeData(id, data)) => {
                    let float_value = data.get_float();
                    let mut env = OwnedEnv::new();
                    let sent = env.send_and_clear(&pid, |env| {
                        (wing_reader_data(), id, float_value).encode(env)
                    });
                    if sent.is_err() {
                        // Owner process is gone — close the connection and
                        // exit instead of reading into the void forever.
                        let _ = thread_handle.wing.shutdown();
                        break;
                    }
                }
                Ok(_) => continue,
                Err(e) => {
                    if !thread_handle.stop.load(Ordering::Acquire) {
                        let msg = format!("{:?}", e);
                        let mut env = OwnedEnv::new();
                        let _ = env.send_and_clear(&pid, |env| {
                            (wing_reader_error(), msg).encode(env)
                        });
                    }
                    break;
                }
            }
        }
    });

    Ok(handle)
}

/// Stop a reader thread: set its stop flag and shut the socket down so a
/// blocked read() wakes immediately. Also closes the console connection
/// (reader and writer share one socket), so only call this when tearing the
/// whole connection down.
#[rustler::nif]
fn stop_reader_thread(handle: ReaderArc) -> rustler::types::atom::Atom {
    handle.stop.store(true, Ordering::Release);
    let _ = handle.wing.shutdown();
    rustler::types::atom::ok()
}

#[rustler::nif]
fn start_meter_thread(host: Option<String>, pid_term: Term, meters_term: Term) -> NifResult<()> {
    let pid: LocalPid = pid_term.decode()?;
    // Accept list of tuples: {atom, integer}
    let meters: Vec<(rustler::types::atom::Atom, u8)> = meters_term.decode()?;
    let meters: Vec<libwing::Meter> = meters.into_iter().map(|(kind, idx)| {
        if kind == channel() {
            libwing::Meter::Channel(idx)
        } else if kind == mix() {
            libwing::Meter::Bus(idx)
        } else if kind == aux() {
            libwing::Meter::Bus(idx)
        } else if kind == main() {
            libwing::Meter::Main(idx)
        } else if kind == matrix() {
            libwing::Meter::Matrix(idx)
        } else {
            libwing::Meter::Channel(1)
        }
    }).collect();

    // Get or create connection
    let console = match WingConsole::connect(host.as_deref()) {
        Ok(conn) => std::sync::Arc::new(std::sync::Mutex::new(conn)),
        Err(_) => return Err(rustler::Error::Term(Box::new("Failed to connect".to_string()))),
    };

    // Start thread to handle meter updates
    thread::spawn(move || {
        // Request meter data
        {
            let mut wing = console.lock().unwrap();
            if let Err(_) = wing.request_meter(&meters) {
                return;
            }
        }

        loop {
            let result = {
                let mut wing = console.lock().unwrap();
                wing.read_meters()
            };

            if let Ok((_id, values)) = result {
                let msg: Vec<i32> = values.iter().map(|v| *v as i32).collect();
                let mut env = OwnedEnv::new();
                let _ = env.send_and_clear(&pid, |env| (rustler::types::atom::ok(), msg).encode(env));
            }
        }
    });

    Ok(())
}

// Legacy per-property reader: spawns a thread with its OWN TCP connection per
// property id and no way to stop it. Wing.Console no longer uses this (see
// start_reader_thread); it remains only for Wing.Fader / Wing.Preamp. Do not
// use in new code — the Wing console only allows a couple of simultaneous
// TCP connections and these threads leak them.
#[rustler::nif]
fn start_property_thread(host: Option<String>, pid_term: Term, prop_id: i32) -> NifResult<()> {
    let pid: LocalPid = pid_term.decode()?;

    // Get or create connection
    let console = match WingConsole::connect(host.as_deref()) {
        Ok(conn) => std::sync::Arc::new(std::sync::Mutex::new(conn)),
        Err(_) => return Err(rustler::Error::Term(Box::new("Failed to connect".to_string()))),
    };

    // Start thread to handle property updates
    thread::spawn(move || {
        // Request initial data
        {
            let mut wing = console.lock().unwrap();
            if let Err(_) = wing.request_node_data(prop_id) {
                return;
            }
        }

        loop {
            let response = {
                let mut wing = console.lock().unwrap();
                wing.read()
            };

            match response {
                Ok(libwing::WingResponse::NodeData(id, data)) => {
                    if id == prop_id {
                        let mut env = OwnedEnv::new();
                        let float_value = data.get_float();
                        let msg = (
                            rustler::types::atom::ok(),
                            id,
                            float_value
                        );
                        let _ = env.send_and_clear(&pid, |env| msg.encode(env));
                    }
                }
                Ok(libwing::WingResponse::RequestEnd) => {
                    // Request end, continue reading
                    continue;
                }
                Ok(libwing::WingResponse::NodeDef(_)) => {
                    // Node definition, continue reading
                    continue;
                }
                Err(_) => {
                    // Connection error, exit thread
                    break;
                }
            }
        }
    });

    Ok(())
}

#[rustler::nif]
fn init_wing_thread(host: Option<String>) -> NifResult<(WingArc, rustler::types::atom::Atom)> {
    match WingConsole::connect(host.as_deref()) {
        Ok(wing_console) => {
            let wing_arc = ResourceArc::new(ExWing {
                wing: Mutex::new(wing_console),
            });
            Ok((wing_arc, rustler::types::atom::ok()))
        }
        Err(e) => {
            let error_msg = format!("Failed to connect: {:?}", e);
            Err(rustler::Error::Term(Box::new(error_msg)))
        }
    }
}

#[rustler::nif]
fn name_to_id(name: String) -> i32 {
    libwing::WingConsole::name_to_id(&name).unwrap_or(-1)
}

// Writes go through the console's existing connection (wsock lock only, so
// they interleave with the reader thread). A blocking write on a stalled
// peer must not stall a normal scheduler — hence DirtyIo.
#[rustler::nif(schedule = "DirtyIo")]
fn set_float(wing_arc: WingArc, id: i32, value: f32) -> Result<(), String> {
    let mut wing = wing_arc.wing.lock().unwrap();
    wing.set_float(id, value).map_err(|e| format!("{:?}", e))
}

#[rustler::nif(schedule = "DirtyIo")]
fn request_node_data(wing_arc: WingArc, id: i32) -> Result<(), String> {
    let mut wing = wing_arc.wing.lock().unwrap();
    wing.request_node_data(id).map_err(|e| format!("{:?}", e))
}

rustler::init!("Elixir.Wing", load = on_load);
