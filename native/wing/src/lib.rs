use libwing::WingNodeData;
use rustler::{ ResourceArc,NifTaggedEnum};
use rustler::{Env, Term, NifResult, Encoder, OwnedEnv, LocalPid};

use libwing::{WingConsole, WingResponse,WingNodeDef,DiscoveryInfo};

use std::sync::Mutex;
use std::thread;

rustler::atoms! {
    channel,
    mix,
    aux,
    main,
    matrix
}

struct ExWing { pub wing: Mutex<WingConsole> }

type WingArc = ResourceArc<ExWing>;

#[derive(NifTaggedEnum)]
pub enum WingResponseSimple {
    RequestEnd,
    NodeDef(WingNodeDef),
    NodeData(i32, WingNodeData),
    NodeDataSimple(String, WingNodeData),
}

fn on_load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(ExWing, env);
    true
}

#[rustler::nif(schedule = "DirtyCpu")]
fn connect() -> WingArc {
    ResourceArc::new(
        ExWing {
            wing: Mutex::new(WingConsole::connect(None).unwrap()),
        }
    )
}

#[rustler::nif(schedule = "DirtyCpu")]
fn connect_with_host(host: Option<String>) -> WingArc {
    // For now, let's not use shared connection to test basic functionality
    ResourceArc::new(
        ExWing {
            wing: Mutex::new(WingConsole::connect(host.as_deref()).unwrap()),
        }
    )
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
        Ok(conn) => conn,
        Err(_) => return Err(rustler::Error::Term(Box::new("Failed to connect".to_string()))),
    };

    // Start thread to handle meter updates with reconnection logic
    thread::spawn(move || {
        let mut wing = console;
        let host_str = host;
        let mut consecutive_errors: u32 = 0;
        const MAX_BACKOFF_MS: u64 = 30_000;

        loop {
            // (Re-)request meter data after connect/reconnect
            match wing.request_meter(&meters) {
                Ok(_) => { consecutive_errors = 0; }
                Err(_) => {
                    // Notify GenServer of connection trouble
                    let mut env = OwnedEnv::new();
                    let _ = env.send_and_clear(&pid, |env| {
                        (rustler::types::atom::error(), "meter_thread_connection_lost").encode(env)
                    });

                    let backoff = std::cmp::min(1000 * 2u64.saturating_pow(consecutive_errors), MAX_BACKOFF_MS);
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    thread::sleep(std::time::Duration::from_millis(backoff));

                    // Try to reconnect
                    match WingConsole::connect(host_str.as_deref()) {
                        Ok(new_wing) => { wing = new_wing; }
                        Err(_) => {}
                    }
                    continue;
                }
            }

            // Read meter data in a loop
            loop {
                match wing.read_meters() {
                    Ok((_id, values)) => {
                        consecutive_errors = 0;
                        let msg: Vec<i32> = values.iter().map(|v| *v as i32).collect();
                        let mut env = OwnedEnv::new();
                        if env.send_and_clear(&pid, |env| (rustler::types::atom::ok(), msg).encode(env)).is_err() {
                            // GenServer process is dead, exit thread
                            return;
                        }
                    }
                    Err(_) => {
                        // Connection lost - notify GenServer and attempt reconnection
                        let mut env = OwnedEnv::new();
                        let _ = env.send_and_clear(&pid, |env| {
                            (rustler::types::atom::error(), "meter_thread_connection_lost").encode(env)
                        });

                        let backoff = std::cmp::min(1000 * 2u64.saturating_pow(consecutive_errors), MAX_BACKOFF_MS);
                        consecutive_errors = consecutive_errors.saturating_add(1);
                        thread::sleep(std::time::Duration::from_millis(backoff));

                        match WingConsole::connect(host_str.as_deref()) {
                            Ok(new_wing) => { wing = new_wing; }
                            Err(_) => {}
                        }
                        break; // Break inner loop to re-request meters on new connection
                    }
                }
            }
        }
    });

    Ok(())
}

#[rustler::nif]
fn start_property_thread(host: Option<String>, pid_term: Term, prop_id: i32) -> NifResult<()> {
    let pid: LocalPid = pid_term.decode()?;

    // Get or create connection
    let console = match WingConsole::connect(host.as_deref()) {
        Ok(conn) => conn,
        Err(_) => return Err(rustler::Error::Term(Box::new("Failed to connect".to_string()))),
    };

    // Start thread to handle property updates with reconnection logic
    thread::spawn(move || {
        let mut wing = console;
        let host_str = host;
        let mut consecutive_errors: u32 = 0;
        const MAX_BACKOFF_MS: u64 = 30_000;

        loop {
            // (Re-)request initial data after connect/reconnect
            match wing.request_node_data(prop_id) {
                Ok(_) => { consecutive_errors = 0; }
                Err(_) => {
                    let mut env = OwnedEnv::new();
                    let _ = env.send_and_clear(&pid, |env| {
                        (rustler::types::atom::error(), "property_thread_connection_lost", prop_id).encode(env)
                    });

                    let backoff = std::cmp::min(1000 * 2u64.saturating_pow(consecutive_errors), MAX_BACKOFF_MS);
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    thread::sleep(std::time::Duration::from_millis(backoff));

                    match WingConsole::connect(host_str.as_deref()) {
                        Ok(new_wing) => { wing = new_wing; }
                        Err(_) => {}
                    }
                    continue;
                }
            }

            // Read property updates
            loop {
                match wing.read() {
                    Ok(libwing::WingResponse::NodeData(id, data)) => {
                        consecutive_errors = 0;
                        if id == prop_id {
                            let mut env = OwnedEnv::new();
                            let float_value = data.get_float();
                            let msg = (
                                rustler::types::atom::ok(),
                                id,
                                float_value
                            );
                            if env.send_and_clear(&pid, |env| msg.encode(env)).is_err() {
                                // GenServer process is dead, exit thread
                                return;
                            }
                        }
                    }
                    Ok(libwing::WingResponse::RequestEnd) => {
                        consecutive_errors = 0;
                        continue;
                    }
                    Ok(libwing::WingResponse::NodeDef(_)) => {
                        consecutive_errors = 0;
                        continue;
                    }
                    Err(_) => {
                        // Connection lost - notify GenServer and attempt reconnection
                        let mut env = OwnedEnv::new();
                        let _ = env.send_and_clear(&pid, |env| {
                            (rustler::types::atom::error(), "property_thread_connection_lost", prop_id).encode(env)
                        });

                        let backoff = std::cmp::min(1000 * 2u64.saturating_pow(consecutive_errors), MAX_BACKOFF_MS);
                        consecutive_errors = consecutive_errors.saturating_add(1);
                        thread::sleep(std::time::Duration::from_millis(backoff));

                        match WingConsole::connect(host_str.as_deref()) {
                            Ok(new_wing) => { wing = new_wing; }
                            Err(_) => {}
                        }
                        break; // Break inner loop to re-request data on new connection
                    }
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

#[rustler::nif]
fn set_float(_wing_arc: WingArc, id: i32, value: f32) -> Result<(), String> {
    // Simplified: use direct connection for now
    match WingConsole::connect(None) {
        Ok(mut wing) => wing.set_float(id, value).map_err(|e| format!("{:?}", e)),
        Err(e) => Err(format!("Connection failed: {:?}", e))
    }
}

#[rustler::nif]
fn request_node_data(_wing_arc: WingArc, id: i32) -> Result<(), String> {
    // Simplified: use direct connection for now  
    match WingConsole::connect(None) {
        Ok(mut wing) => wing.request_node_data(id).map_err(|e| format!("{:?}", e)),
        Err(e) => Err(format!("Connection failed: {:?}", e))
    }
}

rustler::init!("Elixir.Wing", load = on_load);

