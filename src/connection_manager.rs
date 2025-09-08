use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use std::sync::mpsc::{self, Receiver, Sender};
use crate::{WingConsole, WingResponse, Result};

/// Message types for communication between threads and the connection manager
#[derive(Debug, Clone)]
pub enum ConnectionMessage {
    /// Request node data for a property ID
    RequestNodeData(i32),
    /// Request node definition for a property ID  
    RequestNodeDefinition(i32),
    /// Set a string value for a property ID
    SetString(i32, String),
    /// Set a float value for a property ID
    SetFloat(i32, f32),
    /// Set an integer value for a property ID
    SetInt(i32, i32),
    /// Request meter data for specified meters
    RequestMeter(Vec<crate::console::Meter>),
    /// Subscribe to property updates for a specific property ID
    SubscribeProperty(i32, Sender<WingResponse>),
    /// Unsubscribe from property updates
    UnsubscribeProperty(i32),
    /// Subscribe to meter updates
    SubscribeMeter(Sender<(u16, Vec<i16>)>),
    /// Unsubscribe from meter updates
    UnsubscribeMeter,
    /// Disconnect and cleanup
    Disconnect,
}

/// Singleton connection manager that maintains a single connection to the Wing console
pub struct ConnectionManager {
    instance: Arc<Mutex<Option<ConnectionManagerInner>>>,
}

struct ConnectionManagerInner {
    wing_console: WingConsole,
    host: String,
    command_sender: Sender<ConnectionMessage>,
    property_subscribers: HashMap<i32, Vec<Sender<WingResponse>>>,
    meter_subscribers: Vec<Sender<(u16, Vec<i16>)>>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl ConnectionManager {
    /// Get or create the singleton instance
    pub fn instance() -> &'static ConnectionManager {
        static INSTANCE: std::sync::OnceLock<ConnectionManager> = std::sync::OnceLock::new();
        INSTANCE.get_or_init(|| ConnectionManager {
            instance: Arc::new(Mutex::new(None)),
        })
    }

    /// Connect to a Wing console
    pub fn connect(&self, host: &str) -> Result<()> {
        let mut instance = self.instance.lock().unwrap();
        
        // Disconnect existing connection if any
        if let Some(mut inner) = instance.take() {
            let _ = inner.command_sender.send(ConnectionMessage::Disconnect);
            if let Some(handle) = inner.thread_handle.take() {
                let _ = handle.join();
            }
        }

        // Create new connection
        let wing_console = WingConsole::connect(Some(host))?;
        let (command_sender, command_receiver) = mpsc::channel();
        
        let mut inner = ConnectionManagerInner {
            wing_console: wing_console.clone(),
            host: host.to_string(),
            command_sender: command_sender.clone(),
            property_subscribers: HashMap::new(),
            meter_subscribers: Vec::new(),
            thread_handle: None,
        };

        // Start the main communication thread
        let host_clone = host.to_string();
        let handle = thread::spawn(move || {
            Self::run_communication_loop(wing_console, command_receiver, host_clone);
        });

        inner.thread_handle = Some(handle);
        *instance = Some(inner);

        Ok(())
    }

    /// Disconnect from the Wing console
    pub fn disconnect(&self) {
        let mut instance = self.instance.lock().unwrap();
        if let Some(mut inner) = instance.take() {
            let _ = inner.command_sender.send(ConnectionMessage::Disconnect);
            if let Some(handle) = inner.thread_handle.take() {
                let _ = handle.join();
            }
        }
    }

    /// Send a command to the Wing console
    pub fn send_command(&self, command: ConnectionMessage) -> Result<()> {
        let instance = self.instance.lock().unwrap();
        if let Some(inner) = instance.as_ref() {
            inner.command_sender.send(command)
                .map_err(|_| crate::Error::ConnectionError)?;
            Ok(())
        } else {
            Err(crate::Error::ConnectionError)
        }
    }

    /// Subscribe to property updates
    pub fn subscribe_property(&self, property_id: i32, sender: Sender<WingResponse>) -> Result<()> {
        self.send_command(ConnectionMessage::SubscribeProperty(property_id, sender))
    }

    /// Unsubscribe from property updates
    pub fn unsubscribe_property(&self, property_id: i32) -> Result<()> {
        self.send_command(ConnectionMessage::UnsubscribeProperty(property_id))
    }

    /// Subscribe to meter updates
    pub fn subscribe_meter(&self, sender: Sender<(u16, Vec<i16>)>) -> Result<()> {
        self.send_command(ConnectionMessage::SubscribeMeter(sender))
    }

    /// Set a property value
    pub fn set_float(&self, property_id: i32, value: f32) -> Result<()> {
        self.send_command(ConnectionMessage::SetFloat(property_id, value))
    }

    pub fn set_int(&self, property_id: i32, value: i32) -> Result<()> {
        self.send_command(ConnectionMessage::SetInt(property_id, value))
    }

    pub fn set_string(&self, property_id: i32, value: String) -> Result<()> {
        self.send_command(ConnectionMessage::SetString(property_id, value))
    }

    /// Request node data
    pub fn request_node_data(&self, property_id: i32) -> Result<()> {
        self.send_command(ConnectionMessage::RequestNodeData(property_id))
    }

    /// Request node definition
    pub fn request_node_definition(&self, property_id: i32) -> Result<()> {
        self.send_command(ConnectionMessage::RequestNodeDefinition(property_id))
    }

    /// Request meter data
    pub fn request_meter(&self, meters: Vec<crate::console::Meter>) -> Result<()> {
        self.send_command(ConnectionMessage::RequestMeter(meters))
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        let instance = self.instance.lock().unwrap();
        instance.is_some()
    }

    /// Get the current host
    pub fn get_host(&self) -> Option<String> {
        let instance = self.instance.lock().unwrap();
        instance.as_ref().map(|inner| inner.host.clone())
    }

    /// Main communication loop that runs in a separate thread
    fn run_communication_loop(
        mut wing_console: WingConsole,
        command_receiver: Receiver<ConnectionMessage>,
        host: String,
    ) {
        let mut property_subscribers: HashMap<i32, Vec<Sender<WingResponse>>> = HashMap::new();
        let mut meter_subscribers: Vec<Sender<(u16, Vec<i16>)>> = Vec::new();
        let mut should_exit = false;

        // Spawn a thread to handle incoming messages from Wing console
        let wing_clone = wing_console.clone();
        let (response_sender, response_receiver) = mpsc::channel();
        let host_clone1 = host.clone();
        
        thread::spawn(move || {
            let mut wing = wing_clone;
            loop {
                match wing.read() {
                    Ok(response) => {
                        if response_sender.send(response).is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        // Connection lost, try to reconnect
                        thread::sleep(Duration::from_millis(1000));
                        match WingConsole::connect(Some(&host_clone1)) {
                            Ok(new_wing) => {
                                wing = new_wing;
                            }
                            Err(_) => {
                                thread::sleep(Duration::from_millis(5000));
                            }
                        }
                    }
                }
            }
        });

        // Spawn a thread to handle meter data
        let wing_clone2 = wing_console.clone();
        let (meter_sender, meter_receiver) = mpsc::channel();
        let host_clone2 = host.clone();
        
        thread::spawn(move || {
            let mut wing = wing_clone2;
            loop {
                match wing.read_meters() {
                    Ok((id, data)) => {
                        if meter_sender.send((id, data)).is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        // Connection lost, try to reconnect  
                        thread::sleep(Duration::from_millis(1000));
                        match WingConsole::connect(Some(&host_clone2)) {
                            Ok(new_wing) => {
                                wing = new_wing;
                            }
                            Err(_) => {
                                thread::sleep(Duration::from_millis(5000));
                            }
                        }
                    }
                }
            }
        });

        while !should_exit {
            // Handle commands
            while let Ok(command) = command_receiver.try_recv() {
                match command {
                    ConnectionMessage::RequestNodeData(id) => {
                        let _ = wing_console.request_node_data(id);
                    }
                    ConnectionMessage::RequestNodeDefinition(id) => {
                        let _ = wing_console.request_node_definition(id);
                    }
                    ConnectionMessage::SetString(id, value) => {
                        let _ = wing_console.set_string(id, &value);
                    }
                    ConnectionMessage::SetFloat(id, value) => {
                        let _ = wing_console.set_float(id, value);
                    }
                    ConnectionMessage::SetInt(id, value) => {
                        let _ = wing_console.set_int(id, value);
                    }
                    ConnectionMessage::RequestMeter(meters) => {
                        let _ = wing_console.request_meter(&meters);
                    }
                    ConnectionMessage::SubscribeProperty(id, sender) => {
                        property_subscribers.entry(id).or_insert_with(Vec::new).push(sender);
                        let _ = wing_console.request_node_data(id);
                    }
                    ConnectionMessage::UnsubscribeProperty(id) => {
                        property_subscribers.remove(&id);
                    }
                    ConnectionMessage::SubscribeMeter(sender) => {
                        meter_subscribers.push(sender);
                    }
                    ConnectionMessage::UnsubscribeMeter => {
                        meter_subscribers.clear();
                    }
                    ConnectionMessage::Disconnect => {
                        should_exit = true;
                        break;
                    }
                }
            }

            // Handle Wing console responses
            while let Ok(response) = response_receiver.try_recv() {
                match &response {
                    WingResponse::NodeData(id, _) => {
                        if let Some(subscribers) = property_subscribers.get(id) {
                            for sender in subscribers {
                                let _ = sender.send(response.clone());
                            }
                        }
                    }
                    _ => {
                        // Broadcast other responses to all property subscribers
                        for subscribers in property_subscribers.values() {
                            for sender in subscribers {
                                let _ = sender.send(response.clone());
                            }
                        }
                    }
                }
            }

            // Handle meter data
            while let Ok((id, data)) = meter_receiver.try_recv() {
                for sender in &meter_subscribers {
                    let _ = sender.send((id, data.clone()));
                }
            }

            // Small sleep to prevent busy waiting
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for ConnectionManagerInner {
    fn drop(&mut self) {
        let _ = self.command_sender.send(ConnectionMessage::Disconnect);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}
