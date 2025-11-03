# Migration Guide: Legacy Wing Modules to Wing.Console

This guide explains how to migrate from the deprecated `Wing.Meter`, `Wing.Fader`, and `Wing.Preamp` modules to the modern `Wing.Console` API.

## Why Migrate?

The old modules (`Wing.Meter`, `Wing.Fader`, `Wing.Preamp`) each created **separate TCP connections** to the Wing mixer, leading to:
- ❌ Multiple unnecessary connections
- ❌ Resource waste
- ❌ Increased "no route to host" errors
- ❌ Difficult to manage connection lifecycle

The new `Wing.Console` module provides:
- ✅ **Single TCP connection** per console process
- ✅ Centralized subscription management
- ✅ Automatic cleanup and resource management
- ✅ Better error handling and reconnection logic

## Migration Steps

### 1. Migrating from Wing.Meter

**Old Code:**
```elixir
# Start Wing.Meter as a separate process
{:ok, _pid} = Wing.Meter.start_link(
  "192.168.1.100",
  meters: [{:channel, 1}, {:channel, 2}, {:mix, 1}],
  forward_to: self()
)

# Receive meter updates
receive do
  {:ok, meter_values} -> IO.inspect(meter_values)
end
```

**New Code:**
```elixir
# Start a Wing.Console connection
{:ok, console} = Wing.Console.start_link("192.168.1.100")

# Subscribe to meters
Wing.Console.subscribe_meters(
  console,
  [{:channel, 1}, {:channel, 2}, {:mix, 1}],
  self()
)

# Receive meter updates (same format)
receive do
  {:ok, meter_values} -> IO.inspect(meter_values)
end
```

### 2. Migrating from Wing.Fader

**Old Code:**
```elixir
# Connect and subscribe to fader changes
console = Wing.connect_with_host("192.168.1.100")
Wing.Fader.subscribe_channel_fader(console, 1, self())

# Set fader value
Wing.Fader.set_channel_fader(console, 1, -10.0)

# Receive fader change notifications
receive do
  {:fader_changed, :channel, 1, db_value} -> 
    IO.puts("Fader changed to #{db_value} dB")
end
```

**New Code:**
```elixir
# Start a Wing.Console connection
{:ok, console} = Wing.Console.start_link("192.168.1.100")

# Subscribe to fader property
# Channel 1 fader property ID: you can use Wing.name_to_id("/ch/01/fdr")
property_id = Wing.name_to_id("/ch/01/fdr")
Wing.Console.subscribe_property(console, property_id, self())

# Set fader value using Wing API directly
Wing.set_float(console, property_id, fader_value_as_float)

# Receive property change notifications
receive do
  {:property_changed, ^property_id, new_value} -> 
    # Convert float to dB if needed
    db_value = convert_to_db(new_value)
    IO.puts("Fader changed to #{db_value} dB")
end
```

**Note:** You may want to keep using `Wing.Fader` wrapper functions for the high-level API (like `set_channel_fader`), but ensure the underlying implementation uses `Wing.Console` for subscriptions.

### 3. Migrating from Wing.Preamp

**Old Code:**
```elixir
# Connect and subscribe to preamp changes
console = Wing.connect_with_host("192.168.1.100")
Wing.Preamp.subscribe_channel_preamp(console, 1, self())

# Set preamp value
Wing.Preamp.set_channel_preamp(console, 1, 6.0)

# Receive preamp change notifications
receive do
  {:preamp_changed, :channel, 1, db_value} -> 
    IO.puts("Preamp changed to #{db_value} dB")
end
```

**New Code:**
```elixir
# Start a Wing.Console connection
{:ok, console} = Wing.Console.start_link("192.168.1.100")

# Subscribe to preamp property
# Channel 1 preamp property ID: you can use Wing.name_to_id("/ch/01/preamp")
property_id = Wing.name_to_id("/ch/01/preamp")
Wing.Console.subscribe_property(console, property_id, self())

# Set preamp value using Wing API directly
Wing.set_float(console, property_id, preamp_value_as_float)

# Receive property change notifications
receive do
  {:property_changed, ^property_id, new_value} -> 
    db_value = convert_to_db(new_value)
    IO.puts("Preamp changed to #{db_value} dB")
end
```

## Key Changes

### Connection Management

**Old:**
- Each module created its own connection
- No centralized lifecycle management
- Multiple connections to the same mixer

**New:**
- Single `Wing.Console` process per mixer
- Centralized connection management
- All subscriptions share one TCP connection

### Subscription API

**Old:**
```elixir
Wing.Fader.subscribe_channel_fader(console, channel_num, subscriber)
Wing.Preamp.subscribe_channel_preamp(console, channel_num, subscriber)
```

**New:**
```elixir
Wing.Console.subscribe_property(console, property_id, subscriber)
Wing.Console.subscribe_meters(console, meters, subscriber)
```

### Message Format

**Fader/Preamp Messages:**
- **Old:** `{:fader_changed, type, id, db_value}` or `{:preamp_changed, type, id, db_value}`
- **New:** `{:property_changed, property_id, raw_value}` (you may need to convert to dB)

**Meter Messages:**
- **Same:** `{:ok, [meter_value1, meter_value2, ...]}`

## Example: Complete Migration

### Before (Multiple Connections)

```elixir
defmodule MyApp.WingManager do
  use GenServer
  
  def init(_) do
    # Creates 3 separate TCP connections!
    {:ok, _} = Wing.Meter.start_link("192.168.1.100", 
      meters: [{:channel, 1}], forward_to: self())
    
    console = Wing.connect_with_host("192.168.1.100")
    Wing.Fader.subscribe_channel_fader(console, 1, self())
    Wing.Preamp.subscribe_channel_preamp(console, 1, self())
    
    {:ok, %{console: console}}
  end
  
  def handle_info({:ok, meters}, state) do
    # Handle meters
    {:noreply, state}
  end
  
  def handle_info({:fader_changed, :channel, 1, db}, state) do
    # Handle fader
    {:noreply, state}
  end
  
  def handle_info({:preamp_changed, :channel, 1, db}, state) do
    # Handle preamp
    {:noreply, state}
  end
end
```

### After (Single Connection)

```elixir
defmodule MyApp.WingManager do
  use GenServer
  
  def init(_) do
    # Single TCP connection!
    {:ok, console} = Wing.Console.start_link("192.168.1.100")
    
    # Subscribe to all the things
    Wing.Console.subscribe_meters(console, [{:channel, 1}], self())
    
    fader_id = Wing.name_to_id("/ch/01/fdr")
    preamp_id = Wing.name_to_id("/ch/01/preamp")
    
    Wing.Console.subscribe_property(console, fader_id, self())
    Wing.Console.subscribe_property(console, preamp_id, self())
    
    {:ok, %{
      console: console,
      fader_id: fader_id,
      preamp_id: preamp_id
    }}
  end
  
  def handle_info({:ok, meters}, state) do
    # Handle meters (same as before)
    {:noreply, state}
  end
  
  def handle_info({:property_changed, property_id, value}, state) do
    cond do
      property_id == state.fader_id ->
        db = convert_to_db(value)
        # Handle fader change
        {:noreply, state}
        
      property_id == state.preamp_id ->
        db = convert_to_db(value)
        # Handle preamp change
        {:noreply, state}
        
      true ->
        {:noreply, state}
    end
  end
end
```

## Backward Compatibility

For the transition period, the old modules are **deprecated but still functional**:

- ✅ `Wing.Meter` - Still works, now uses `Wing.Console` internally
- ⚠️ `Wing.Fader` - Still works but creates separate connections (should be migrated)
- ⚠️ `Wing.Preamp` - Still works but creates separate connections (should be migrated)

However, for optimal performance and resource usage, **migrate to `Wing.Console`** as soon as possible.

## Testing Migration

1. **Verify connection count:**
   ```elixir
   # Before migration: multiple connections
   Wing.scan() # Shows multiple connections from same IP
   
   # After migration: single connection
   Wing.scan() # Shows only one connection
   ```

2. **Monitor resource usage:**
   ```elixir
   # Check process count
   length(Process.list())
   
   # Check memory usage
   :erlang.memory()
   ```

3. **Test functionality:**
   - Ensure all subscriptions still receive updates
   - Verify meter values are correct
   - Test fader/preamp changes
   - Confirm error handling works

## Need Help?

If you encounter issues during migration:

1. Check the `Wing.Console` module documentation
2. Review the test files in `test/wing_console_test.exs`
3. Look at examples in the mediacontrol application
4. File an issue on GitHub

## Timeline

- **v1.0.x**: Legacy modules deprecated, backward compatibility maintained
- **v2.0.0**: Legacy modules will be removed (planned)

Start migrating now to avoid breaking changes in v2.0!
