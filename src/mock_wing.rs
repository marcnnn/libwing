//! Mock Wing Console implementation for testing error conditions
//! 
//! This module provides a mock implementation of the Wing console that can simulate
//! various error conditions for comprehensive testing.

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream, UdpSocket, SocketAddr};
use std::io::{Read, Write, Result as IoResult, ErrorKind};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};

// Mock Wing server for testing - imports removed to avoid unused warnings

/// Mock Wing Console configuration for testing different scenarios
#[derive(Debug, Clone)]
pub struct MockWingConfig {
    /// Should the mock respond to discovery packets
    pub respond_to_discovery: bool,
    /// Should connections be accepted
    pub accept_connections: bool,
    /// Should TCP connections randomly drop
    pub simulate_connection_drops: bool,
    /// Should responses be delayed
    pub simulate_slow_responses: bool,
    /// Response delay duration
    pub response_delay: Duration,
    /// Should invalid data be sent
    pub send_invalid_data: bool,
    /// Should the mock hang on certain operations
    pub simulate_hangs: bool,
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Properties that exist on this mock console
    pub available_properties: HashMap<i32, f32>,
}

impl Default for MockWingConfig {
    fn default() -> Self {
        let mut properties = HashMap::new();
        // Add some default test properties
        properties.insert(950957506, -20.0); // Channel 1 fader
        properties.insert(125312537, -20.0); // Channel 2 fader
        properties.insert(-534056607, -20.0); // Channel 3 fader
        
        Self {
            respond_to_discovery: true,
            accept_connections: true,
            simulate_connection_drops: false,
            simulate_slow_responses: false,
            response_delay: Duration::from_millis(10),
            send_invalid_data: false,
            simulate_hangs: false,
            max_connections: 10,
            available_properties: properties,
        }
    }
}

/// Mock Wing Console Server
pub struct MockWingServer {
    config: Arc<Mutex<MockWingConfig>>,
    discovery_socket: Option<UdpSocket>,
    tcp_listener: Option<TcpListener>,
    running: Arc<AtomicBool>,
    connection_count: Arc<Mutex<usize>>,
    pub address: SocketAddr,
}

impl MockWingServer {
    /// Create a new mock Wing server
    pub fn new(config: MockWingConfig) -> IoResult<Self> {
        let tcp_listener = TcpListener::bind("127.0.0.1:0")?;
        let address = tcp_listener.local_addr()?;
        
        let discovery_socket = UdpSocket::bind("127.0.0.1:0").ok();
        
        Ok(Self {
            config: Arc::new(Mutex::new(config)),
            discovery_socket,
            tcp_listener: Some(tcp_listener),
            running: Arc::new(AtomicBool::new(false)),
            connection_count: Arc::new(Mutex::new(0)),
            address,
        })
    }
    
    /// Start the mock server
    pub fn start(&mut self) -> IoResult<()> {
        self.running.store(true, Ordering::SeqCst);
        
        // Start discovery responder
        if let Some(discovery_socket) = &self.discovery_socket {
            let socket = discovery_socket.try_clone()?;
            let config = self.config.clone();
            let running = self.running.clone();
            let address = self.address;
            
            thread::spawn(move || {
                Self::handle_discovery(socket, config, running, address);
            });
        }
        
        // Start TCP listener
        if let Some(listener) = self.tcp_listener.take() {
            let config = self.config.clone();
            let running = self.running.clone();
            let connection_count = self.connection_count.clone();
            
            thread::spawn(move || {
                Self::handle_tcp_connections(listener, config, running, connection_count);
            });
        }
        
        Ok(())
    }
    
    /// Stop the mock server
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
    
    /// Update configuration during runtime
    pub fn update_config<F>(&self, f: F) 
    where 
        F: FnOnce(&mut MockWingConfig)
    {
        if let Ok(mut config) = self.config.lock() {
            f(&mut *config);
        }
    }
    
    /// Get current connection count
    pub fn connection_count(&self) -> usize {
        self.connection_count.lock().map(|c| *c).unwrap_or(0)
    }
    
    fn handle_discovery(
        socket: UdpSocket, 
        config: Arc<Mutex<MockWingConfig>>, 
        running: Arc<AtomicBool>,
        address: SocketAddr
    ) {
        let mut buf = [0u8; 1024];
        socket.set_read_timeout(Some(Duration::from_millis(100))).ok();
        
        while running.load(Ordering::SeqCst) {
            if let Ok((size, src)) = socket.recv_from(&mut buf) {
                let config_guard = config.lock().unwrap();
                if !config_guard.respond_to_discovery {
                    continue;
                }
                
                // Simple discovery response simulation
                if size > 0 && buf[0] == 0x01 { // Assuming discovery packet starts with 0x01
                    let response = format!(
                        "WING-MOCK,{},MockWing,MOCK123,1.0.0", 
                        address.ip()
                    );
                    let _ = socket.send_to(response.as_bytes(), src);
                }
            }
        }
    }
    
    fn handle_tcp_connections(
        listener: TcpListener,
        config: Arc<Mutex<MockWingConfig>>,
        running: Arc<AtomicBool>,
        connection_count: Arc<Mutex<usize>>
    ) {
        listener.set_nonblocking(true).ok();
        
        while running.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    let current_count = {
                        let mut count = connection_count.lock().unwrap();
                        *count += 1;
                        *count
                    };
                    
                    let config_guard = config.lock().unwrap();
                    if !config_guard.accept_connections || current_count > config_guard.max_connections {
                        drop(stream);
                        continue;
                    }
                    
                    let config_clone = config.clone();
                    let running_clone = running.clone();
                    let connection_count_clone = connection_count.clone();
                    
                    thread::spawn(move || {
                        Self::handle_client_connection(stream, config_clone, running_clone);
                        
                        // Decrement connection count when done
                        if let Ok(mut count) = connection_count_clone.lock() {
                            *count -= 1;
                        }
                    });
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    }
    
    fn handle_client_connection(
        mut stream: TcpStream,
        config: Arc<Mutex<MockWingConfig>>,
        running: Arc<AtomicBool>
    ) {
        stream.set_read_timeout(Some(Duration::from_millis(100))).ok();
        stream.set_write_timeout(Some(Duration::from_millis(100))).ok();
        
        let mut buffer = [0u8; 1024];
        let mut last_heartbeat = Instant::now();
        
        while running.load(Ordering::SeqCst) {
            let config_guard = config.lock().unwrap();
            
            // Simulate connection drops
            if config_guard.simulate_connection_drops && rand::random::<f32>() < 0.01 {
                break;
            }
            
            // Simulate hangs
            if config_guard.simulate_hangs && rand::random::<f32>() < 0.005 {
                thread::sleep(Duration::from_secs(10));
                continue;
            }
            
            // Handle incoming data
            match stream.read(&mut buffer) {
                Ok(0) => break, // Connection closed
                Ok(size) => {
                    Self::process_wing_command(&mut stream, &buffer[..size], &config_guard);
                    last_heartbeat = Instant::now();
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    // Check for heartbeat timeout
                    if last_heartbeat.elapsed() > Duration::from_secs(30) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    }
    
    fn process_wing_command(stream: &mut TcpStream, data: &[u8], config: &MockWingConfig) {
        if config.simulate_slow_responses {
            thread::sleep(config.response_delay);
        }
        
        if config.send_invalid_data && rand::random::<f32>() < 0.1 {
            // Send invalid/corrupted data 10% of the time
            let invalid_data = [0xFF, 0xFE, 0xFD, 0xFC];
            let _ = stream.write_all(&invalid_data);
            return;
        }
        
        // Simulate Wing protocol responses based on commands
        Self::simulate_wing_protocol_response(stream, data, config);
    }
    
    fn simulate_wing_protocol_response(stream: &mut TcpStream, data: &[u8], config: &MockWingConfig) {
        // This is a simplified simulation of Wing protocol responses
        // In reality, Wing protocol is more complex, but this is sufficient for testing
        
        if data.is_empty() {
            return;
        }
        
        match data[0] {
            // Simulate property request
            0x01 => {
                if data.len() >= 5 {
                    let prop_id = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                    if let Some(&value) = config.available_properties.get(&prop_id) {
                        // Send property data response
                        let response = Self::create_property_response(prop_id, value);
                        let _ = stream.write_all(&response);
                    }
                }
            }
            // Simulate property set
            0x02 => {
                if data.len() >= 9 {
                    let _prop_id = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                    let _value = f32::from_le_bytes([data[5], data[6], data[7], data[8]]);
                    // Just acknowledge the set (Wing doesn't typically respond to sets)
                }
            }
            // Simulate meter request
            0x03 => {
                // Send meter data
                let meter_response = Self::create_meter_response();
                let _ = stream.write_all(&meter_response);
            }
            _ => {
                // Unknown command, send error or ignore
            }
        }
    }
    
    fn create_property_response(prop_id: i32, value: f32) -> Vec<u8> {
        let mut response = Vec::new();
        response.push(0x10); // Property data response marker
        response.extend_from_slice(&prop_id.to_le_bytes());
        response.extend_from_slice(&value.to_le_bytes());
        response
    }
    
    fn create_meter_response() -> Vec<u8> {
        let mut response = Vec::new();
        response.push(0x20); // Meter data response marker
        response.extend_from_slice(&1u16.to_le_bytes()); // Meter ID
        
        // Simulate 8 channels of meter data
        for i in 0..8 {
            let level = (i * 10) as i16; // Simulate different levels
            response.extend_from_slice(&level.to_le_bytes());
        }
        response
    }
}

impl Drop for MockWingServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Create a mock Wing server for testing specific error conditions
pub fn create_error_test_server(error_type: &str) -> IoResult<MockWingServer> {
    let config = match error_type {
        "connection_refused" => MockWingConfig {
            accept_connections: false,
            ..Default::default()
        },
        "discovery_timeout" => MockWingConfig {
            respond_to_discovery: false,
            ..Default::default()
        },
        "connection_drops" => MockWingConfig {
            simulate_connection_drops: true,
            ..Default::default()
        },
        "slow_responses" => MockWingConfig {
            simulate_slow_responses: true,
            response_delay: Duration::from_secs(2),
            ..Default::default()
        },
        "invalid_data" => MockWingConfig {
            send_invalid_data: true,
            ..Default::default()
        },
        "hangs" => MockWingConfig {
            simulate_hangs: true,
            ..Default::default()
        },
        "max_connections" => MockWingConfig {
            max_connections: 1,
            ..Default::default()
        },
        _ => MockWingConfig::default(),
    };
    
    MockWingServer::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_mock_server_creation() {
        let server = MockWingServer::new(MockWingConfig::default());
        assert!(server.is_ok());
    }
    
    #[test]
    fn test_config_updates() {
        let server = MockWingServer::new(MockWingConfig::default()).unwrap();
        
        server.update_config(|config| {
            config.accept_connections = false;
        });
        
        // Verify the configuration was updated
        let config_guard = server.config.lock().unwrap();
        assert!(!config_guard.accept_connections);
    }
    
    #[test]
    fn test_error_server_creation() {
        let test_cases = [
            "connection_refused",
            "discovery_timeout", 
            "connection_drops",
            "slow_responses",
            "invalid_data",
            "hangs",
            "max_connections"
        ];
        
        for error_type in &test_cases {
            let server = create_error_test_server(error_type);
            assert!(server.is_ok(), "Failed to create server for error type: {}", error_type);
        }
    }
}
