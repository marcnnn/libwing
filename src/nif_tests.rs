//! NIF Layer Error Condition Tests
//! 
//! Tests the Elixir NIF interface error handling and ensures that Rust panics
//! don't crash the Erlang VM.

// Removed unused imports
use std::thread;
use std::time::Duration;

use crate::mock_wing::{MockWingServer, MockWingConfig};

/// Test the NIF functions with error conditions
#[cfg(test)]
mod nif_error_tests {
    use super::*;

    #[test]
    fn test_connect_with_invalid_host() {
        // Test connection with invalid host formats
        let invalid_hosts = vec![
            "invalid_host",
            "256.256.256.256:8080",  // Invalid IP
            "127.0.0.1:99999",       // Invalid port
            "127.0.0.1:-1",          // Negative port
            "",                      // Empty string
            "localhost:abc",         // Non-numeric port
        ];

        for invalid_host in invalid_hosts {
            // In the actual NIF, this would be tested via Elixir
            // Here we simulate what the NIF layer should handle
            let result = crate::WingConsole::connect(Some(invalid_host));
            
            assert!(result.is_err(), 
                "Expected error for invalid host: {}", invalid_host);
        }
    }

    #[test]
    fn test_concurrent_nif_operations() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let address = format!("127.0.0.1:{}", server.address.port());
        
        // Simulate multiple Elixir processes making concurrent NIF calls
        let handles: Vec<_> = (0..10).map(|thread_id| {
            let address_clone = address.clone();
            
            thread::spawn(move || {
                // Simulate the connect_with_host NIF
                let console_result = crate::WingConsole::connect(Some(&address_clone));
                
                match console_result {
                    Ok(mut console) => {
                        // Simulate set_float NIF calls
                        for i in 0..5 {
                            let prop_id = 950957506 + (thread_id * 10) + i;
                            let value = -20.0 + i as f32;
                            
                            let result = console.set_float(prop_id, value);
                            match result {
                                Ok(_) => {}, // Success
                                Err(e) => {
                                    println!("Thread {} set_float error: {:?}", thread_id, e);
                                }
                            }
                        }
                        
                        // Simulate request_node_data NIF calls
                        for i in 0..3 {
                            let prop_id = 950957506 + i;
                            let result = console.request_node_data(prop_id);
                            match result {
                                Ok(_) => {},
                                Err(e) => {
                                    println!("Thread {} request_node_data error: {:?}", thread_id, e);
                                }
                            }
                        }
                        
                        // Simulate read NIF calls
                        for _ in 0..3 {
                            match console.read() {
                                Ok(_response) => {},
                                Err(e) => {
                                    println!("Thread {} read error: {:?}", thread_id, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Thread {} connect error: {:?}", thread_id, e);
                    }
                }
            })
        }).collect();
        
        // Wait for all threads to complete
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(_) => {},
                Err(e) => {
                    panic!("Thread {} panicked: {:?}", i, e);
                }
            }
        }
        
        println!("All concurrent NIF operations completed successfully");
    }

    #[test]
    fn test_resource_cleanup_on_error() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let address = format!("127.0.0.1:{}", server.address.port());
        
        // Create connections that will be dropped due to errors
        let mut successful_connections = 0;
        let mut failed_connections = 0;
        
        for i in 0..20 {
            match crate::WingConsole::connect(Some(&address)) {
                Ok(mut console) => {
                    successful_connections += 1;
                    
                    // Perform some operations that might fail
                    let _ = console.set_float(i, f32::NAN);
                    let _ = console.request_node_data(i32::MIN);
                    
                    // Force connection drop by dropping the console
                    drop(console);
                }
                Err(_) => {
                    failed_connections += 1;
                }
            }
            
            // Brief pause to allow cleanup
            thread::sleep(Duration::from_millis(10));
        }
        
        println!("Resource cleanup test: {} successful, {} failed connections", 
                successful_connections, failed_connections);
        
        // Allow time for cleanup
        thread::sleep(Duration::from_millis(500));
        
        // All connections should be cleaned up
        assert_eq!(server.connection_count(), 0);
    }

    #[test] 
    fn test_error_propagation() {
        // Test that Rust errors are properly converted to Elixir errors
        
        // Connection errors
        let conn_result = crate::WingConsole::connect(Some("192.168.255.255:1234"));
        assert!(conn_result.is_err());
        
        match conn_result.unwrap_err() {
            crate::Error::ConnectionError => {
                // Expected error type
            }
            crate::Error::Io(_) => {
                // Also acceptable
            }
            other => {
                panic!("Unexpected error type: {:?}", other);
            }
        }
        
        // Invalid property operations should be handled gracefully
        // (These would normally be tested via the NIF interface)
    }

    #[test]
    fn test_memory_safety() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let address = format!("127.0.0.1:{}", server.address.port());
        
        // Test memory safety with rapid allocation/deallocation
        for _ in 0..100 {
            if let Ok(mut console) = crate::WingConsole::connect(Some(&address)) {
                // Perform operations that allocate memory
                let _ = console.request_node_data(950957506);
                
                // Force immediate drop to test cleanup
                drop(console);
            }
        }
        
        // Should not crash or leak memory
        println!("Memory safety test completed");
    }
}

/// Test error conditions in the meter functionality
#[cfg(test)]
mod meter_nif_tests {
    use super::*;
    use crate::console::Meter;

    #[test]
    fn test_meter_nif_errors() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let address = format!("127.0.0.1:{}", server.address.port());
        
        if let Ok(mut console) = crate::WingConsole::connect(Some(&address)) {
            // Test meter operations that might fail
            let test_meter_configs = vec![
                vec![], // Empty meter list
                vec![Meter::Channel(1)], // Single meter
                vec![Meter::Channel(255)], // Extreme channel
                (0..50).map(|i| Meter::Channel(i)).collect(), // Large meter list
            ];
            
            for meters in test_meter_configs {
                match console.request_meter(&meters) {
                    Ok(meter_id) => {
                        println!("Meter request succeeded with {} meters, ID: {}", 
                                meters.len(), meter_id);
                        
                        // Try to read meter data
                        for _ in 0..3 {
                            match console.read_meters() {
                                Ok((id, values)) => {
                                    println!("Received meter data: ID {}, {} values", id, values.len());
                                    break;
                                }
                                Err(e) => {
                                    println!("Meter read error: {:?}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Meter request with {} meters failed: {:?}", meters.len(), e);
                    }
                }
            }
        }
    }
}

/// Test the property thread functionality with error conditions
#[cfg(test)]
mod property_thread_tests {
    use super::*;

    #[test]
    fn test_property_thread_error_handling() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let address = format!("127.0.0.1:{}", server.address.port());
        
        // Test property thread with various error conditions
        let test_properties = vec![
            950957506,  // Valid property
            0,          // Potentially invalid
            -1,         // Invalid
            i32::MAX,   // Extreme value
            i32::MIN,   // Extreme value
        ];
        
        for prop_id in test_properties {
            if let Ok(mut console) = crate::WingConsole::connect(Some(&address)) {
                match console.request_node_data(prop_id) {
                    Ok(_) => {
                        println!("Property request for {} succeeded", prop_id);
                        
                        // Try to read the response
                        for _ in 0..5 {
                            match console.read() {
                                Ok(response) => {
                                    println!("Received response for property {}: {:?}", prop_id, response);
                                    break;
                                }
                                Err(e) => {
                                    println!("Read error for property {}: {:?}", prop_id, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Property request for {} failed: {:?}", prop_id, e);
                    }
                }
            }
        }
    }
}

/// Test name to ID conversion with error conditions
#[cfg(test)]
mod name_conversion_tests {
    // use super::*; // Removed unused import

    #[test]
    fn test_name_to_id_errors() {
        // Test name to ID conversion with various invalid inputs
        let invalid_names = vec![
            "",
            "invalid_property_name",
            "!@#$%^&*()",
            "very_long_property_name_that_should_not_exist_in_any_reasonable_system",
            "\0null_byte",
            "unicode_测试_名称",
        ];
        
        for invalid_name in invalid_names {
            let result = crate::WingConsole::name_to_id(invalid_name);
            
            // Should return -1 for invalid names (based on the NIF implementation)
            assert_eq!(result, Some(-1), 
                "Expected -1 for invalid name: {}", invalid_name);
        }
        
        // Test some valid names if any are known
        // This would depend on the actual property map
    }
}

    /// Comprehensive stress test for all NIF functions
#[cfg(test)]
mod stress_tests {
    use super::*;

    #[test]
    fn test_nif_stress() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let address = format!("127.0.0.1:{}", server.address.port());
        
        // Stress test with rapid operations
        let console_result = crate::WingConsole::connect(Some(&address));
        if let Ok(mut console) = console_result {
            // Rapid property sets
            for i in 0..1000 {
                let prop_id = 950957506 + (i % 100);
                let value = -30.0 + (i as f32 * 0.1);
                
                match console.set_float(prop_id, value) {
                    Ok(_) => {},
                    Err(e) => {
                        if i % 100 == 0 {
                            println!("Set error at iteration {}: {:?}", i, e);
                        }
                    }
                }
                
                // Occasional reads
                if i % 50 == 0 {
                    match console.read() {
                        Ok(_) => {},
                        Err(e) => {
                            println!("Read error at iteration {}: {:?}", i, e);
                        }
                    }
                }
            }
            
            println!("Stress test completed successfully");
        }
    }

    #[test]
    fn test_concurrent_stress() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let address = format!("127.0.0.1:{}", server.address.port());
        
        // Multiple threads performing stress operations
        let handles: Vec<_> = (0..5).map(|thread_id| {
            let address_clone = address.clone();
            
            thread::spawn(move || {
                if let Ok(mut console) = crate::WingConsole::connect(Some(&address_clone)) {
                    for i in 0..200 {
                        let prop_id = 950957506 + (thread_id * 1000) + i;
                        let value = -20.0 + (i as f32 * 0.1);
                        
                        // Mix of operations
                        match i % 3 {
                            0 => {
                                let _ = console.set_float(prop_id, value);
                            }
                            1 => {
                                let _ = console.request_node_data(prop_id);
                            }
                            2 => {
                                let _ = console.read();
                            }
                            _ => unreachable!(),
                        }
                        
                        if i % 50 == 0 {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
            })
        }).collect();
        
        // Wait for all stress threads
        for handle in handles {
            handle.join().expect("Stress thread panicked");
        }
        
        println!("Concurrent stress test completed");
    }
}
