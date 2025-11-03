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
    #[allow(non_local_definitions)]
    {
        let _ = rustler::resource!(ExWing, env);
    }
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
fn connect_with_host(host: Option<String>) -> NifResult<WingArc> {
    // For now, let's not use shared connection to test basic functionality
    match WingConsole::connect(host.as_deref()) {
        Ok(wing_console) => {
            Ok(ResourceArc::new(
                ExWing {
                    wing: Mutex::new(wing_console),
                }
            ))
        }
        Err(e) => {
            let error_msg = format!("Failed to connect to Wing console: {:?}", e);
            Err(rustler::Error::Term(Box::new(error_msg)))
        }
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn read_simple(wing_arc: WingArc) -> NifResult<WingResponseSimple> {
    let mut wing = wing_arc.wing.lock()
        .map_err(|_| rustler::Error::Term(Box::new("Failed to lock wing mutex".to_string())))?;
    let wing: &mut WingConsole = &mut *wing;

    loop {
        if let Ok(WingResponse::NodeData(id, data)) =  wing.read() {
            match WingConsole::id_to_defs(id) {
                
                Some(defs) if defs.is_empty() => return Ok(WingResponseSimple::NodeData(id, data)),
                Some(defs) if defs.len() == 1 => {
                    let longname = format!("{}", defs[0].0);
                    return Ok(WingResponseSimple::NodeDataSimple(longname, data));
                }
                Some(defs) if (defs.len() > 1) => continue,
                Some(_) => continue,
                None => continue,
            }
        }
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn read(wing_arc: WingArc) -> NifResult<WingResponse> {
    let mut wing = wing_arc.wing.lock()
        .map_err(|_| rustler::Error::Term(Box::new("Failed to lock wing mutex".to_string())))?;
    let wing: &mut WingConsole = &mut *wing;

    loop {
        if let Ok(response) = wing.read() {
            return Ok(response);
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
fn start_meter_thread_arc(wing_arc: WingArc, pid_term: Term, meters_term: Term) -> NifResult<()> {
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
    
    // Clone the shared connection resource
    let console = wing_arc.clone();
    
    // Start thread to handle meter updates
    thread::spawn(move || {
        let mut consecutive_errors = 0;
        let max_errors = 10;
        
        // Request meter data
        {
            if let Ok(mut wing) = console.wing.lock() {
                if let Err(_) = wing.request_meter(&meters) {
                    return;
                }
            } else {
                return;
            }
        }
        
        loop {
            let result = {
                console.wing.lock()
                    .ok()
                    .and_then(|mut wing| wing.read_meters().ok())
            };
            
            if let Some((_id, values)) = result {
                consecutive_errors = 0; // Reset error counter on success
                
                let msg: Vec<i32> = values.iter().map(|v| *v as i32).collect();
                let mut env = OwnedEnv::new();
                let _ = env.send_and_clear(&pid, |env| (rustler::types::atom::ok(), msg).encode(env));
                
                // Small delay to prevent hammering when many meter updates arrive
                // Meters update frequently, so use a very short delay
                std::thread::sleep(std::time::Duration::from_micros(500));
            } else {
                // Failed to read, exponential backoff
                consecutive_errors += 1;
                
                if consecutive_errors >= max_errors {
                    // Too many consecutive errors, give up
                    return;
                }
                
                let delay_ms = std::cmp::min(100 * consecutive_errors, 1000);
                std::thread::sleep(std::time::Duration::from_millis(delay_ms));
            }
        }
    });
    
    Ok(())
}

#[rustler::nif]
fn start_unified_property_thread(wing_arc: WingArc, pid_term: Term) -> NifResult<()> {
    let pid: LocalPid = pid_term.decode()?;
    
    // Clone the shared connection resource
    let console = wing_arc.clone();
    
    // Start a single thread to handle ALL property updates
    thread::spawn(move || {
        let mut consecutive_errors = 0;
        let max_errors = 10;
        
        loop {
            let response = {
                console.wing.lock()
                    .ok()
                    .and_then(|mut wing| wing.read().ok())
            };
            
            match response {
                Some(libwing::WingResponse::NodeData(id, data)) => {
                    consecutive_errors = 0; // Reset error counter on success
                    
                    // Send all property updates to the GenServer
                    // GenServer will filter and dispatch to appropriate subscribers
                    let mut env = OwnedEnv::new();
                    let float_value = data.get_float();
                    let msg = (
                        rustler::types::atom::ok(),
                        id,
                        float_value
                    );
                    let _ = env.send_and_clear(&pid, |env| msg.encode(env));
                    
                    // Small delay to prevent overwhelming the GenServer
                    std::thread::sleep(std::time::Duration::from_micros(100));
                }
                Some(libwing::WingResponse::RequestEnd) => {
                    // Request end, small delay before continuing
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Some(libwing::WingResponse::NodeDef(_)) => {
                    // Node definition, small delay before continuing
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                None => {
                    // Connection error, exponential backoff
                    consecutive_errors += 1;
                    
                    if consecutive_errors >= max_errors {
                        // Too many errors, notify GenServer and exit
                        let mut env = OwnedEnv::new();
                        let msg = (
                            rustler::types::atom::error(),
                            "connection_lost"
                        );
                        let _ = env.send_and_clear(&pid, |env| msg.encode(env));
                        return;
                    }
                    
                    let delay_ms = std::cmp::min(100 * consecutive_errors, 1000);
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                }
            }
        }
    });
    
    Ok(())
}

#[rustler::nif]
fn start_property_thread_arc(wing_arc: WingArc, pid_term: Term, prop_id: i32) -> NifResult<()> {
    let pid: LocalPid = pid_term.decode()?;
    
    // Clone the shared connection resource
    let console = wing_arc.clone();
    
    // Start thread to handle property updates
    thread::spawn(move || {
        let mut consecutive_errors = 0;
        let max_errors = 10;
        
        // Request initial data
        {
            if let Ok(mut wing) = console.wing.lock() {
                if let Err(_) = wing.request_node_data(prop_id) {
                    return;
                }
            } else {
                return;
            }
        }
        
        loop {
            let response = {
                console.wing.lock()
                    .ok()
                    .and_then(|mut wing| wing.read().ok())
            };
            
            match response {
                Some(libwing::WingResponse::NodeData(id, data)) => {
                    consecutive_errors = 0; // Reset error counter on success
                    
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
                    // Small delay to prevent hammering the mutex when many messages arrive
                    std::thread::sleep(std::time::Duration::from_micros(100));
                }
                Some(libwing::WingResponse::RequestEnd) => {
                    // Request end, small delay before continuing
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                Some(libwing::WingResponse::NodeDef(_)) => {
                    // Node definition, small delay before continuing
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                None => {
                    // Connection error, exponential backoff
                    consecutive_errors += 1;
                    
                    if consecutive_errors >= max_errors {
                        // Too many errors, give up
                        return;
                    }
                    
                    let delay_ms = std::cmp::min(100 * consecutive_errors, 1000);
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
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
fn set_float(wing_arc: WingArc, id: i32, value: f32) -> Result<(), String> {
    let mut wing = wing_arc.wing.lock()
        .map_err(|_| "Failed to lock wing mutex".to_string())?;
    wing.set_float(id, value).map_err(|e| format!("{:?}", e))
}

#[rustler::nif]
fn request_node_data(wing_arc: WingArc, id: i32) -> Result<(), String> {
    let mut wing = wing_arc.wing.lock()
        .map_err(|_| "Failed to lock wing mutex".to_string())?;
    wing.request_node_data(id).map_err(|e| format!("{:?}", e))
}

rustler::init!("Elixir.Wing", load = on_load);

