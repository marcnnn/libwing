defmodule Wing.Meter do
  use GenServer

  @moduledoc """
  Convenience wrapper that opens a `Wing.Console`, subscribes to a set of
  meters and forwards every meter frame to another process.

  Frames arrive as `{:meters_updated, %{meters: meters, values: values}}` — see
  `Wing.Console.subscribe_meters/3` for the payload format.

  Owning a console per meter view is wasteful (the Wing allows only a couple of
  TCP connections): if you already have a `Wing.Console`, subscribe to it
  directly instead of using this module.
  """

  @type meter_type :: {:channel, non_neg_integer} | {:mix, non_neg_integer}

  def start_link(host \\ nil, opts \\ []) do
    forward_to = Keyword.get(opts, :forward_to, self())
    meters = Keyword.get(opts, :meters, default_meters())
    GenServer.start_link(__MODULE__, {host, forward_to, meters}, name: __MODULE__)
  end

  defp default_meters, do: Enum.map(1..16, &{:channel, &1})

  @doc """
  Example: meters = [{:channel, 1}, {:channel, 2}, {:mix, 1}]
  """
  def init({host, forward_to, meters}) do
    Process.flag(:trap_exit, true)

    case Wing.Console.start_link(host) do
      {:ok, console} ->
        :ok = Wing.Console.subscribe_meters(console, meters)
        {:ok, %{console: console, forward_to: forward_to, meters: meters}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_info({:meters_updated, payload}, state) do
    send(state.forward_to, {:meters_updated, payload})
    {:noreply, state}
  end

  def handle_info({:wing_connection_lost, reason}, state) do
    {:stop, {:shutdown, {:connection_lost, reason}}, state}
  end

  def handle_info(msg, state) do
    IO.inspect(msg, label: "Unknown message")
    {:noreply, state}
  end

  def terminate(_reason, state) do
    # The console traps exits, so it will not go away with us on its own —
    # stop it explicitly or its TCP connection stays open.
    if Process.alive?(state.console), do: Wing.Console.stop(state.console)
    :ok
  end
end
