defmodule Wing.DiscoveryInfo do
  defstruct ip: "", name: "", model: "", serial: "", firmware: ""
end

defmodule Wing.Console do
  @moduledoc """
  GenServer that manages a Wing console connection.

  One console process owns:
  - a single TCP connection to the console (the NIF resource)
  - a single reader thread sharing that same connection, forwarding every
    property update to this process, which routes them to subscribers

  Property subscriptions are therefore free: subscribing adds an entry to a
  routing table — no extra threads, no extra TCP connections (the Wing only
  allows a couple of simultaneous connections).

  Meters work the same way: at most one meter thread per console, sharing the
  same connection. It runs only while someone is subscribed and always requests
  the union of every subscriber's meters. See `subscribe_meters/3` for the
  message format.

  When the connection dies, the reader thread reports it; this process sends
  `{:wing_connection_lost, reason}` to every subscriber and stops with
  `{:shutdown, :connection_lost}`, so an owner that links or monitors gets a
  prompt, unambiguous signal.
  """

  use GenServer
  require Logger

  @type console_ref :: pid() | atom()
  @type property_id :: integer()
  @type subscriber :: pid()

  # Client API

  @doc """
  Start a console GenServer for the given host.
  """
  @spec start_link(String.t()) :: {:ok, pid()} | {:error, term()}
  def start_link(host) do
    GenServer.start_link(__MODULE__, host)
  end

  @doc """
  Start a named console GenServer for the given host.
  """
  @spec start_link(String.t(), atom()) :: {:ok, pid()} | {:error, term()}
  def start_link(host, name) when is_atom(name) do
    GenServer.start_link(__MODULE__, host, name: name)
  end

  @doc """
  Subscribe to property changes for a specific property ID.
  The subscriber receives `{:ok, property_id, value}` messages on changes and
  `{:wing_connection_lost, reason}` if the console connection dies.
  """
  @spec subscribe_property(console_ref(), property_id(), subscriber()) :: :ok | {:error, term()}
  def subscribe_property(console, property_id, subscriber \\ self()) do
    GenServer.call(console, {:subscribe_property, property_id, subscriber})
  end

  @doc """
  Unsubscribe from property changes for a specific property ID.
  """
  @spec unsubscribe_property(console_ref(), property_id(), subscriber()) :: :ok
  def unsubscribe_property(console, property_id, subscriber \\ self()) do
    GenServer.call(console, {:unsubscribe_property, property_id, subscriber})
  end

  @doc """
  Subscribe to meter updates for the given meters, e.g. `[{:channel, 1}, {:main, 1}]`.

  Subscribers receive, at roughly 21 Hz:

      {:meters_updated, %{meters: [{:channel, 1}, {:main, 1}], values: [...]}}

  `values` is one flat list of integers for the *merged* meter set of all
  current subscribers — that merged set is what `meters` names, in request
  order, which is why it is sent along: it is the only way to tell which value
  belongs to which meter. Each meter contributes 8 values, so meter `n` (zero
  based) in `meters` owns `Enum.slice(values, n * 8, 8)`. Values are dB * 256
  (divide by 256 for dB); -32768 means -inf (silence).

  Subscribing while the meter thread already runs is fine: if the subscription
  adds meters, the merged set is re-requested and every subscriber sees the
  wider payload from then on. Subscribers are monitored; when the last one goes
  away the meter thread stops.
  """
  @spec subscribe_meters(console_ref(), list(), subscriber()) :: :ok | {:error, term()}
  def subscribe_meters(console, meters, subscriber \\ self()) do
    GenServer.call(console, {:subscribe_meters, meters, subscriber})
  end

  @doc """
  Stop receiving meter updates. Stops the meter thread if this was the last
  meter subscriber.
  """
  @spec unsubscribe_meters(console_ref(), subscriber()) :: :ok
  def unsubscribe_meters(console, subscriber \\ self()) do
    GenServer.call(console, {:unsubscribe_meters, subscriber})
  end

  @doc """
  Set a float property value.
  """
  @spec set_float(console_ref(), property_id(), float()) :: :ok | {:error, term()}
  def set_float(console, property_id, value) do
    GenServer.call(console, {:set_float, property_id, value})
  end

  @doc """
  Get the raw Wing console reference for direct NIF calls.
  """
  @spec get_console_ref(console_ref()) :: reference()
  def get_console_ref(console) do
    GenServer.call(console, :get_console_ref)
  end

  @doc """
  Stop the console GenServer (closes the connection and reader thread).
  """
  @spec stop(console_ref()) :: :ok
  def stop(console) do
    GenServer.stop(console)
  end

  @doc """
  Execute a function with automatic reconnection on broken pipe errors.
  This is a global wrapper that can be used for any Wing operation.
  """
  @spec with_reconnection(console_ref(), function()) :: term()
  def with_reconnection(console, operation_fn) do
    case operation_fn.() do
      {:error, {:error, error_msg}} when is_binary(error_msg) ->
        if String.contains?(error_msg, "Broken pipe") do
          Logger.warning("Broken pipe detected, attempting reconnection")

          case reconnect(console) do
            :ok ->
              Logger.info("Reconnection successful, retrying operation")
              operation_fn.()

            {:error, reason} ->
              Logger.error("Reconnection failed: #{inspect(reason)}")
              {:error, {:error, error_msg}}
          end
        else
          {:error, {:error, error_msg}}
        end

      other_result ->
        other_result
    end
  end

  @doc """
  Reconnect the console to its host, keeping all subscriptions.
  """
  @spec reconnect(console_ref()) :: :ok | {:error, term()}
  def reconnect(console) do
    GenServer.call(console, :reconnect)
  end

  # GenServer callbacks

  @impl true
  def init(host) do
    Process.flag(:trap_exit, true)

    case Wing.connect_with_host(host) do
      {:ok, console_ref} ->
        reader_handle = Wing.start_reader_thread(console_ref, self())

        state = %{
          console_ref: console_ref,
          reader_handle: reader_handle,
          host: host,
          property_subscriptions: %{},
          meter_subscriptions: [],
          meter_handle: nil,
          meter_set: [],
          monitored_pids: %{}
        }

        {:ok, state}

      {:error, reason} ->
        {:stop, {:connection_failed, reason}}
    end
  end

  @impl true
  def handle_call({:subscribe_property, property_id, subscriber}, _from, state) do
    # Monitor the subscriber process so its subscriptions are cleaned up
    ref = Process.monitor(subscriber)
    monitored_pids = Map.put(state.monitored_pids, ref, subscriber)

    current_subs = Map.get(state.property_subscriptions, property_id, [])
    new_subs = [subscriber | current_subs] |> Enum.uniq()
    property_subscriptions = Map.put(state.property_subscriptions, property_id, new_subs)

    # No thread to start: the single reader thread already sees every property
    # update on this connection; subscribing only adds a routing entry.
    # Request the node's data so a current value (if the console reports one)
    # reaches new subscribers promptly.
    _ = Wing.request_node_data(state.console_ref, property_id)

    {:reply, :ok,
     %{state | property_subscriptions: property_subscriptions, monitored_pids: monitored_pids}}
  end

  @impl true
  def handle_call({:unsubscribe_property, property_id, subscriber}, _from, state) do
    current_subs = Map.get(state.property_subscriptions, property_id, [])
    new_subs = Enum.reject(current_subs, &(&1 == subscriber))

    property_subscriptions =
      if Enum.empty?(new_subs) do
        Map.delete(state.property_subscriptions, property_id)
      else
        Map.put(state.property_subscriptions, property_id, new_subs)
      end

    {:reply, :ok, %{state | property_subscriptions: property_subscriptions}}
  end

  @impl true
  def handle_call({:subscribe_meters, meters, subscriber}, _from, state) do
    # Monitor the subscriber process
    ref = Process.monitor(subscriber)
    monitored_pids = Map.put(state.monitored_pids, ref, subscriber)

    # Add meter subscription
    meter_subscriptions = [{subscriber, meters} | state.meter_subscriptions] |> Enum.uniq()

    # Starting the thread or widening its meter set both happen here, so a
    # subscription that arrives after the thread is running is not ignored.
    case sync_meter_thread(%{state | meter_subscriptions: meter_subscriptions}) do
      {:ok, state} ->
        {:reply, :ok, %{state | monitored_pids: monitored_pids}}

      {:error, reason} ->
        Process.demonitor(ref, [:flush])
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:unsubscribe_meters, subscriber}, _from, state) do
    meter_subscriptions =
      Enum.reject(state.meter_subscriptions, fn {sub, _} -> sub == subscriber end)

    {_, state} = sync_meter_thread(%{state | meter_subscriptions: meter_subscriptions})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:set_float, property_id, value}, _from, state) do
    case Wing.set_float(state.console_ref, property_id, value) do
      {:ok, _} -> {:reply, :ok, state}
      error -> {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_call(:get_console_ref, _from, state) do
    {:reply, state.console_ref, state}
  end

  @impl true
  def handle_call(:reconnect, _from, state) do
    Logger.info("Reconnecting console to #{state.host}")

    # Tear down the old reader thread and connection first — the Wing only
    # has a couple of connection slots, so the old one must be closed before
    # a new one is opened.
    if state.reader_handle, do: Wing.stop_reader_thread(state.reader_handle)
    # The meter thread rides on the old connection too, so it has to go with it.
    if state.meter_handle, do: Wing.stop_meter_thread(state.meter_handle)

    case Wing.connect_with_host(state.host) do
      {:ok, new_console_ref} ->
        new_reader = Wing.start_reader_thread(new_console_ref, self())

        # Re-request data for all subscribed properties on the new connection
        Enum.each(Map.keys(state.property_subscriptions), fn prop_id ->
          _ = Wing.request_node_data(new_console_ref, prop_id)
        end)

        # Same for meters: the merged set is re-requested on the new connection.
        {_, state} =
          sync_meter_thread(%{
            state
            | console_ref: new_console_ref,
              meter_handle: nil,
              meter_set: []
          })

        Logger.info("Successfully reconnected to Wing console at #{state.host}")
        {:reply, :ok, %{state | console_ref: new_console_ref, reader_handle: new_reader}}

      {:error, reason} ->
        Logger.error("Failed to reconnect to Wing console: #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:wing_reader_data, property_id, value}, state) do
    # Property update from the reader thread. The reader forwards every
    # NodeData on the connection; only subscribed ids are routed onward.
    Logger.debug("Wing.Console property update id=#{property_id} value=#{inspect(value)}")

    if Process.whereis(Wing.Fader) do
      send(Wing.Fader, {:property_changed, property_id, value})
    end

    if Process.whereis(Wing.Preamp) do
      send(Wing.Preamp, {:property_changed, property_id, value})
    end

    case Map.get(state.property_subscriptions, property_id) do
      nil -> :ok
      subs -> Enum.each(subs, fn pid -> send(pid, {:ok, property_id, value}) end)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:wing_reader_error, reason}, state) do
    Logger.warning("Wing console connection lost: #{inspect(reason)}")

    # Tell every subscriber explicitly, then stop. Owners that link/monitor
    # this process additionally see the {:shutdown, :connection_lost} exit.
    state.property_subscriptions
    |> Enum.flat_map(fn {_prop_id, subs} -> subs end)
    |> Kernel.++(Enum.map(state.meter_subscriptions, fn {sub, _meters} -> sub end))
    |> Enum.uniq()
    |> Enum.each(fn pid -> send(pid, {:wing_connection_lost, reason}) end)

    {:stop, {:shutdown, :connection_lost}, state}
  end

  @impl true
  def handle_info({:wing_meter_data, values}, state) when is_list(values) do
    # Meter frame from the meter thread. Every subscriber gets the whole merged
    # payload plus the meter set it belongs to (see subscribe_meters/3) — the
    # values carry no labels of their own.
    payload = %{meters: state.meter_set, values: values}

    for {subscriber, _meters} <- state.meter_subscriptions do
      send(subscriber, {:meters_updated, payload})
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    # Remove subscriptions for dead process
    {subscriber, monitored_pids} = Map.pop(state.monitored_pids, ref)

    if subscriber do
      # Remove from property subscriptions
      property_subscriptions =
        state.property_subscriptions
        |> Enum.map(fn {prop_id, subs} ->
          {prop_id, Enum.reject(subs, &(&1 == subscriber))}
        end)
        |> Enum.reject(fn {_prop_id, subs} -> Enum.empty?(subs) end)
        |> Map.new()

      # Remove from meter subscriptions
      meter_subscriptions =
        Enum.reject(state.meter_subscriptions, fn {sub, _meters} ->
          sub == subscriber
        end)

      # Narrows the meter set, or stops the meter thread if that was the last
      # meter subscriber.
      {_, state} =
        sync_meter_thread(%{
          state
          | property_subscriptions: property_subscriptions,
            meter_subscriptions: meter_subscriptions,
            monitored_pids: monitored_pids
        })

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(msg, state) do
    Logger.debug("Wing.Console received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Stop both threads and close the TCP connection deterministically —
    # relying on NIF resource GC would keep a Wing connection slot occupied
    # for an unbounded time.
    if state.meter_handle, do: Wing.stop_meter_thread(state.meter_handle)
    if state.reader_handle, do: Wing.stop_reader_thread(state.reader_handle)
    :ok
  end

  # Single place that owns the meter thread: keeps it running with exactly the
  # union of all current subscriptions' meters, and not running at all when
  # nobody is subscribed.
  #
  # Subscriptions are kept newest-first, so reversing gives subscription order:
  # meters keep their position in the merged set when later subscribers add to
  # it, and the indices existing subscribers computed stay valid.
  defp sync_meter_thread(state) do
    merged =
      state.meter_subscriptions
      |> Enum.reverse()
      |> Enum.flat_map(fn {_sub, meters} -> meters end)
      |> Enum.uniq()

    cond do
      merged == state.meter_set ->
        {:ok, state}

      merged == [] ->
        Wing.stop_meter_thread(state.meter_handle)
        {:ok, %{state | meter_handle: nil, meter_set: []}}

      state.meter_handle != nil ->
        Wing.update_meter_thread(state.meter_handle, merged)
        {:ok, %{state | meter_set: merged}}

      true ->
        try do
          handle = Wing.start_meter_thread(state.console_ref, self(), merged)
          {:ok, %{state | meter_handle: handle, meter_set: merged}}
        rescue
          e ->
            Logger.error("Failed to start Wing meter thread: #{inspect(e)}")
            {:error, e}
        end
    end
  end
end
