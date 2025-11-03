use std::sync::{Arc, Mutex, OnceLock};
use crate::WingConsole;

/// A simple shared Wing console connection
pub struct SharedWingConnection {
    console: Arc<Mutex<WingConsole>>,
    host: String,
}

static SHARED_CONNECTION: OnceLock<Mutex<Option<SharedWingConnection>>> = OnceLock::new();

impl SharedWingConnection {
    /// Get or create the shared connection
    pub fn get_or_connect(host: Option<&str>) -> Result<Arc<Mutex<WingConsole>>, crate::Error> {
        let instance = SHARED_CONNECTION.get_or_init(|| Mutex::new(None));
        let mut guard = instance.lock()
            .map_err(|_| crate::Error::ConnectionError)?;
        
        match guard.as_ref() {
            Some(conn) => {
                // Check if we need to reconnect to a different host
                if let Some(h) = host {
                    if conn.host != h {
                        // Connect to new host
                        let console = WingConsole::connect(Some(h))?;
                        *guard = Some(SharedWingConnection {
                            console: Arc::new(Mutex::new(console)),
                            host: h.to_string(),
                        });
                        return Ok(guard.as_ref()
                            .ok_or(crate::Error::ConnectionError)?
                            .console.clone());
                    }
                } 
                // Return existing connection
                Ok(conn.console.clone())
            }
            None => {
                // Create new connection
                let host_str = host.unwrap_or("auto");
                let console = if host_str == "auto" {
                    WingConsole::connect(None)?
                } else {
                    WingConsole::connect(Some(host_str))?
                };
                
                let shared = SharedWingConnection {
                    console: Arc::new(Mutex::new(console)),
                    host: host_str.to_string(),
                };
                
                let console_clone = shared.console.clone();
                *guard = Some(shared);
                Ok(console_clone)
            }
        }
    }

    /// Clear the shared connection (for disconnection)
    pub fn disconnect() {
        if let Some(instance) = SHARED_CONNECTION.get() {
            if let Ok(mut guard) = instance.lock() {
                *guard = None;
            }
        }
    }
}
