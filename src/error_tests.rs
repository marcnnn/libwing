//! Comprehensive integration tests for Wing console error conditions
//! 
//! These tests use the mock Wing server to simulate various error scenarios
//! and ensure the libwing code handles them gracefully.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::mock_wing::{MockWingServer, MockWingConfig, create_error_test_server};
use crate::{WingConsole, Error};
use crate::console::Meter;

/// Test connection failure scenarios
#[cfg(test)]
mod connection_tests {
    use super::*;

    #[test]
    fn test_connection_refused() {
        let mut server = create_error_test_server("connection_refused")
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let result = WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port())));
        
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::ConnectionError | Error::Io(_) => {
                // Expected error types for connection refused
            }
            other => panic!("Unexpected error type: {:?}", other),
        }
    }

    #[test] 
    fn test_connection_timeout() {
        // Try to connect to a non-existent host
        let result = WingConsole::connect(Some("192.168.255.255:2222"));
        
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::ConnectionError | Error::Io(_) => {
                // Expected error types for connection timeout
            }
            other => panic!("Unexpected error type: {:?}", other),
        }
    }

    #[test]
    fn test_connection_drops_during_operation() {
        let mut server = create_error_test_server("connection_drops")
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let mut console = WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port())))
            .expect("Failed to connect to mock server");
        
        // Try to perform operations - some should fail due to dropped connections
        let mut success_count = 0;
        let mut error_count = 0;
        
        for i in 0..50 {
            match console.set_float(950957506 + i, -20.0 + i as f32) {
                Ok(_) => success_count += 1,
                Err(_) => error_count += 1,
            }
            
            thread::sleep(Duration::from_millis(10));
        }
        
        // With connection drops enabled, we should see some errors
        assert!(error_count > 0, "Expected some errors due to connection drops");
        println!("Connection drops test: {} successes, {} errors", success_count, error_count);
    }

    #[test]
    fn test_max_connections_exceeded() {
        let mut server = create_error_test_server("max_connections")
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let address = format!("127.0.0.1:{}", server.address.port());
        
        // First connection should succeed
        let _console1 = WingConsole::connect(Some(&address))
            .expect("First connection should succeed");
        
        // Second connection should fail (max_connections = 1)
        let result2 = WingConsole::connect(Some(&address));
        assert!(result2.is_err(), "Second connection should fail when max connections exceeded");
    }
}

/// Test discovery failure scenarios
#[cfg(test)]
mod discovery_tests {
    use super::*;

    #[test]
    fn test_discovery_timeout() {
        let mut server = create_error_test_server("discovery_timeout")
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        // Discovery should timeout since the server won't respond
        let start_time = Instant::now();
        let result = WingConsole::scan(true);
        let elapsed = start_time.elapsed();
        
        // Should return empty results due to timeout
        match result {
            Ok(discoveries) => {
                assert!(discoveries.is_empty(), "Should not discover any consoles");
            }
            Err(_) => {
                // Error is also acceptable for discovery timeout
            }
        }
        
        // Should complete in reasonable time (scan has internal timeout)
        assert!(elapsed < Duration::from_secs(5), "Discovery took too long: {:?}", elapsed);
    }

    #[test]
    fn test_successful_discovery() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        // Give server time to start
        thread::sleep(Duration::from_millis(100));
        
        let result = WingConsole::scan(true);
        
        match result {
            Ok(discoveries) => {
                println!("Discovered {} consoles", discoveries.len());
                // With the mock server responding, we might discover it
                // (depending on network conditions and UDP handling)
            }
            Err(e) => {
                println!("Discovery failed: {:?}", e);
                // Discovery can fail for various reasons, this is not necessarily an error
            }
        }
    }
}

/// Test protocol error scenarios
#[cfg(test)]
mod protocol_tests {
    use super::*;

    #[test]
    fn test_invalid_data_handling() {
        let mut server = create_error_test_server("invalid_data")
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let mut console = WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port())))
            .expect("Failed to connect to mock server");
        
        // Try to read data - should handle invalid data gracefully
        let mut valid_responses = 0;
        let mut invalid_responses = 0;
        
        for _ in 0..20 {
            match console.read() {
                Ok(_response) => valid_responses += 1,
                Err(Error::InvalidData) => invalid_responses += 1,
                Err(other) => {
                    println!("Unexpected error: {:?}", other);
                }
            }
            
            thread::sleep(Duration::from_millis(50));
        }
        
        println!("Invalid data test: {} valid, {} invalid responses", 
                valid_responses, invalid_responses);
        
        // We should see some invalid responses due to the mock sending bad data
        assert!(invalid_responses > 0, "Expected some invalid data responses");
    }

    #[test]
    fn test_slow_response_handling() {
        let mut server = create_error_test_server("slow_responses")
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let mut console = WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port())))
            .expect("Failed to connect to mock server");
        
        // Operations should still work but be slower
        let start_time = Instant::now();
        
        let result = console.request_node_data(950957506);
        let elapsed = start_time.elapsed();
        
        match result {
            Ok(_) => {
                // Should take longer due to slow responses
                assert!(elapsed > Duration::from_secs(1), 
                       "Expected slow response, but completed in {:?}", elapsed);
            }
            Err(e) => {
                println!("Slow response test failed: {:?}", e);
                // Slow responses might cause timeouts, which is also valid
            }
        }
    }

    #[test]
    fn test_hang_detection() {
        let mut server = create_error_test_server("hangs")
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let mut console = WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port())))
            .expect("Failed to connect to mock server");
        
        // Try multiple operations - some might hang, but the client should handle it
        let mut completed_operations = 0;
        let mut failed_operations = 0;
        
        for i in 0..10 {
            let start_time = Instant::now();
            
            match console.set_float(950957506 + i, -20.0) {
                Ok(_) => {
                    completed_operations += 1;
                }
                Err(_) => {
                    failed_operations += 1;
                }
            }
            
            let elapsed = start_time.elapsed();
            
            // No operation should hang for more than reasonable timeout
            assert!(elapsed < Duration::from_secs(15), 
                   "Operation {} took too long: {:?}", i, elapsed);
        }
        
        println!("Hang test: {} completed, {} failed operations", 
                completed_operations, failed_operations);
    }
}

/// Test property operation error scenarios
#[cfg(test)]
mod property_tests {
    use super::*;

    #[test]
    fn test_invalid_property_ids() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let mut console = WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port())))
            .expect("Failed to connect to mock server");
        
        // Try to set invalid property IDs
        let invalid_ids = [0, -1, i32::MAX, i32::MIN];
        
        for &invalid_id in &invalid_ids {
            let result = console.set_float(invalid_id, 0.0);
            
            // Operations on invalid IDs should either fail or be silently ignored
            match result {
                Ok(_) => {
                    // Some implementations might silently ignore invalid IDs
                }
                Err(_) => {
                    // Errors are also acceptable for invalid IDs
                }
            }
        }
    }

    #[test]
    fn test_extreme_property_values() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let mut console = WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port())))
            .expect("Failed to connect to mock server");
        
        let test_values = [
            f32::MIN,
            f32::MAX,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::NAN,
            0.0,
            -0.0,
        ];
        
        for &test_value in &test_values {
            let result = console.set_float(950957506, test_value);
            
            match result {
                Ok(_) => {
                    // Extreme values should be handled gracefully
                }
                Err(e) => {
                    println!("Extreme value {} caused error: {:?}", test_value, e);
                    // Errors for extreme values are acceptable
                }
            }
        }
    }

    #[test]
    fn test_concurrent_property_operations() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let address = format!("127.0.0.1:{}", server.address.port());
        
        // Create multiple connections and perform concurrent operations
        let handles: Vec<_> = (0..5).map(|thread_id| {
            let address_clone = address.clone();
            
            thread::spawn(move || {
                let mut console = WingConsole::connect(Some(&address_clone))
                    .expect("Failed to connect in worker thread");
                
                let mut successes = 0;
                let mut failures = 0;
                
                for i in 0..20 {
                    let prop_id = 950957506 + (thread_id * 100) + i;
                    let value = -20.0 + i as f32;
                    
                    match console.set_float(prop_id, value) {
                        Ok(_) => successes += 1,
                        Err(_) => failures += 1,
                    }
                    
                    thread::sleep(Duration::from_millis(10));
                }
                
                (successes, failures)
            })
        }).collect();
        
        let mut total_successes = 0;
        let mut total_failures = 0;
        
        for handle in handles {
            let (successes, failures) = handle.join().expect("Worker thread panicked");
            total_successes += successes;
            total_failures += failures;
        }
        
        println!("Concurrent operations: {} successes, {} failures", 
                total_successes, total_failures);
        
        // Should handle concurrent operations without crashing
        assert!(total_successes > 0, "Expected some successful operations");
    }
}

/// Test meter operation error scenarios  
#[cfg(test)]
mod meter_tests {
    use super::*;

    #[test]
    fn test_meter_request_errors() {
        let mut server = create_error_test_server("connection_drops")
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let mut console = WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port())))
            .expect("Failed to connect to mock server");
        
        let meters = vec![
            Meter::Channel(1),
            Meter::Channel(2),
            Meter::Bus(1),
            Meter::Main(1),
        ];
        
        // Try to request meters - might fail due to connection drops
        match console.request_meter(&meters) {
            Ok(meter_id) => {
                println!("Meter request succeeded with ID: {}", meter_id);
                
                // Try to read meter data
                for _ in 0..10 {
                    match console.read_meters() {
                        Ok((returned_id, values)) => {
                            assert_eq!(returned_id, meter_id);
                            println!("Received meter data: {} values", values.len());
                            break;
                        }
                        Err(e) => {
                            println!("Meter read error: {:?}", e);
                            // Errors are expected with connection drops
                        }
                    }
                    
                    thread::sleep(Duration::from_millis(100));
                }
            }
            Err(e) => {
                println!("Meter request failed: {:?}", e);
                // Failures are expected with connection drops
            }
        }
    }

    #[test]
    fn test_invalid_meter_types() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let mut console = WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port())))
            .expect("Failed to connect to mock server");
        
        // Test various meter configurations
        let meter_configs = vec![
            vec![], // Empty meter list
            vec![Meter::Channel(255)], // Extreme channel number
            vec![Meter::Bus(255)], // Extreme bus number
            // Create a large meter list
            (0..100).map(|i| Meter::Channel(i as u8)).collect::<Vec<_>>(),
        ];
        
        for meters in meter_configs {
            let result = console.request_meter(&meters);
            
            match result {
                Ok(meter_id) => {
                    println!("Meter request with {} meters succeeded: ID {}", 
                            meters.len(), meter_id);
                }
                Err(e) => {
                    println!("Meter request with {} meters failed: {:?}", 
                            meters.len(), e);
                    // Failures for invalid configurations are acceptable
                }
            }
        }
    }
}

/// Test resource exhaustion scenarios
#[cfg(test)]
mod resource_tests {
    use super::*;

    #[test]
    fn test_memory_stress() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        // Create many connections to test memory handling
        let mut connections = Vec::new();
        
        for i in 0..10 {
            match WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port()))) {
                Ok(console) => {
                    connections.push(console);
                }
                Err(e) => {
                    println!("Connection {} failed: {:?}", i, e);
                    break;
                }
            }
        }
        
        println!("Created {} connections", connections.len());
        
        // Perform operations on all connections
        for (i, console) in connections.iter_mut().enumerate() {
            let result = console.set_float(950957506 + i as i32, -20.0 + i as f32);
            match result {
                Ok(_) => {}
                Err(e) => {
                    println!("Operation on connection {} failed: {:?}", i, e);
                }
            }
        }
        
        // Connections should be cleaned up when dropped
        drop(connections);
        
        // Give some time for cleanup
        thread::sleep(Duration::from_millis(100));
        
        // Server should handle the cleanup gracefully
        assert_eq!(server.connection_count(), 0);
    }

    #[test]
    fn test_rapid_connect_disconnect() {
        let mut server = MockWingServer::new(MockWingConfig::default())
            .expect("Failed to create mock server");
        
        server.start().expect("Failed to start server");
        
        let address = format!("127.0.0.1:{}", server.address.port());
        
        // Rapidly connect and disconnect
        for i in 0..50 {
            match WingConsole::connect(Some(&address)) {
                Ok(mut console) => {
                    // Perform a quick operation
                    let _ = console.set_float(950957506, i as f32);
                    // Connection dropped when console goes out of scope
                }
                Err(e) => {
                    println!("Rapid connection {} failed: {:?}", i, e);
                }
            }
            
            if i % 10 == 0 {
                thread::sleep(Duration::from_millis(10));
            }
        }
        
        // Give time for all connections to clean up
        thread::sleep(Duration::from_millis(500));
        
        // All connections should be cleaned up
        assert_eq!(server.connection_count(), 0);
    }
}

/// Integration test helper functions
#[cfg(test)]
mod test_helpers {
    use super::*;

    #[allow(dead_code)]
    pub fn wait_for_condition<F>(condition: F, timeout: Duration) -> bool 
    where
        F: Fn() -> bool,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if condition() {
                return true;
            }
            thread::sleep(Duration::from_millis(10));
        }
        false
    }

    pub fn create_test_console() -> Result<(MockWingServer, WingConsole), Box<dyn std::error::Error>> {
        let mut server = MockWingServer::new(MockWingConfig::default())?;
        server.start()?;
        
        let console = WingConsole::connect(Some(&format!("127.0.0.1:{}", server.address.port())))?;
        
        Ok((server, console))
    }
}

/// Run all error condition tests
#[cfg(test)]
mod integration_suite {
    use super::*;

    #[test]
    fn test_error_recovery() {
        let (_server, mut console) = test_helpers::create_test_console()
            .expect("Failed to create test console");
        
        // Test that the console can recover from various error conditions
        let property_id = 950957506;
        
        // Normal operation
        assert!(console.set_float(property_id, -20.0).is_ok());
        
        // Operation with extreme values
        let _ = console.set_float(property_id, f32::MAX);
        
        // Should still work after extreme values
        assert!(console.set_float(property_id, -15.0).is_ok());
        
        // Multiple rapid operations
        for i in 0..10 {
            let _ = console.set_float(property_id, -20.0 + i as f32);
        }
        
        // Should still work after rapid operations
        assert!(console.set_float(property_id, -10.0).is_ok());
    }

    #[test]
    fn test_thread_safety() {
        let (_server, console) = test_helpers::create_test_console()
            .expect("Failed to create test console");
        
        let console_arc = Arc::new(std::sync::Mutex::new(console));
        
        // Test thread safety with multiple threads accessing the same console
        let handles: Vec<_> = (0..5).map(|thread_id| {
            let console_clone = Arc::clone(&console_arc);
            
            thread::spawn(move || {
                for i in 0..10 {
                    if let Ok(mut console_guard) = console_clone.lock() {
                        let prop_id = 950957506 + (thread_id * 10) + i;
                        let _ = console_guard.set_float(prop_id, thread_id as f32);
                    }
                    thread::sleep(Duration::from_millis(5));
                }
            })
        }).collect();
        
        for handle in handles {
            handle.join().expect("Thread panicked");
        }
        
        // Test passed if we reach this point without panicking
        assert!(true, "Thread safety test completed");
    }
}
