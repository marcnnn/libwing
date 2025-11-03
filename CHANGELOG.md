# Change Log

## [1.0.5] - 2025-11-03

### Safety & Stability Improvements
- **Error Handling Overhaul**: Eliminated 50+ `.unwrap()` calls that could crash the BEAM VM
  - All mutex locks now properly handle poisoned mutexes
  - Time calculations use `checked_duration_since()` to prevent panics
  - Thread-spawned code has graceful error handling
- **TCP Connection Improvements**: 
  - Added SO_KEEPALIVE with 7-second intervals
  - Added connection and read timeouts (30s/10s)
  - Added SO_REUSEADDR for better port reuse
  - Fixed "no route to host" connection issues

### Performance & Architecture
- **Connection Multiplexing**: Removed duplicate connection creation
  - `Wing.Console` now uses single TCP connection per process
  - Removed 120 lines of backward-compatible code
  - 32% code reduction in connection management
- **Protocol Constants**: Replaced magic numbers with named constants
  - Added 23 protocol command constants (CMD_INT16, CMD_FLOAT32, etc.)
  - Improved code readability and maintainability

### API Changes
- **Simplified NIF API**: 
  - Removed host-based NIFs (`start_meter_thread_host`, `start_property_thread_host`)
  - Only arc-based NIFs remain (`start_meter_thread_arc`, `start_property_thread_arc`)
  - `Wing.Console` is now the recommended API for all operations
- **Deprecated Modules**:
  - `Wing.Meter` deprecated in favor of `Wing.Console.subscribe_meters/3`
  - See MIGRATION_GUIDE.md for migration instructions

### Documentation
- Added MIGRATION_GUIDE.md for upgrading from legacy APIs
- Updated README.md with recent improvements section
- Cleaned up temporary documentation files

## [1.0.4] - 2025-03-04

- removed eframe dependency from libwing

## [1.0.3] - 2025-03-03

- Close read socket when dropping WingConsole

## [1.0.2] - 2025-03-03

- Made public fields of WingConsoleHandle and ResponseHandle

## [1.0.1] - 2025-03-03

- Exposed ffi WingConsoleHandle and ResponseHandle so you can write async wrappers around read()

## [1.0.0] - 2025-03-03

- Calls are thread safe now
- NodeData no longer contains a channel ID (the first parameter), which was always the same before.
- Updates docs

## [unreleased] - 2025-02-17
- Added meters API

