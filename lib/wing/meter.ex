defmodule Wing.Meter do
  @moduledoc """
  **DEPRECATED**: This module is deprecated. Use `Wing.Console.subscribe_meters/3` instead.
  
  Legacy module that starts a meter thread via Rust NIF and prints incoming meter values.
  
  For new code, use `Wing.Console` which provides centralized connection management:
  
      # Start a Wing console connection
      {:ok, console} = Wing.Console.start_link(host)
      
      # Subscribe to meters
      Wing.Console.subscribe_meters(console, [{:channel, 1}, {:channel, 2}], self())
  """
  
  use GenServer
  require Logger

  @type meter_type :: {:channel, non_neg_integer} | {:mix, non_neg_integer}

  @deprecated "Use Wing.Console.subscribe_meters/3 instead"
  def start_link(host \\ nil, opts \\ []) do
    Logger.warning("Wing.Meter is deprecated. Please use Wing.Console.subscribe_meters/3 instead.")
    forward_to = Keyword.get(opts, :forward_to, self())
    meters = Keyword.get(opts, :meters, default_meters())
    GenServer.start_link(__MODULE__, {host, forward_to, meters}, name: __MODULE__)
  end

  defp default_meters, do: Enum.map(1..16, &{:channel, &1})

  @doc """
  Example: meters = [{:channel, 1}, {:channel, 2}, {:mix, 1}]
  
  **DEPRECATED**: Use `Wing.Console` instead.
  """
  def init({host, forward_to, meters}) do
    # For backward compatibility, create a connection and start meter thread
    # This uses the old inefficient approach of creating a separate connection
    case Wing.connect_with_host(host) do
      {:ok, console_ref} ->
        case Wing.start_meter_thread_arc(console_ref, self(), meters) do
          :ok -> {:ok, %{forward_to: forward_to, meters: meters, console_ref: console_ref}}
          error -> {:stop, {:failed_to_start_meter_thread, error}}
        end
      error ->
        {:stop, {:failed_to_connect, error}}
    end
  end

  def handle_info({:ok, values}, state) when is_list(values) do
    send(state.forward_to, {:ok, values})
    {:noreply, state}
  end

  def handle_info(msg, state) do
    IO.inspect(msg, label: "Unknown message")
    {:noreply, state}
  end
end
