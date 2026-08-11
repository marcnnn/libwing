defmodule Wing.ConsoleLifecycleTest do
  # These tests run a fake Wing console (a plain TCP server) on the Wing
  # port 2222 on localhost — no hardware needed. async: false because the
  # port is shared global state.
  use ExUnit.Case, async: false

  @wing_port 2222

  setup do
    # Wing.Console links to its caller; a console stopping with
    # {:shutdown, :connection_lost} (or a failed init) must not take the
    # test process down with it.
    Process.flag(:trap_exit, true)
    :ok
  end

  defp with_fake_console!(fun) do
    {:ok, listen} =
      :gen_tcp.listen(@wing_port, [
        :binary,
        active: false,
        reuseaddr: true,
        ip: {127, 0, 0, 1}
      ])

    try do
      fun.(listen)
    after
      :gen_tcp.close(listen)
    end
  end

  # Accept the console's connection inside a task, then hand the socket to
  # `owner` — a gen_tcp socket is closed when its controlling process (the
  # task) exits, which would look like the console hanging up.
  defp accept!(listen, owner) do
    {:ok, sock} = :gen_tcp.accept(listen, 5_000)
    # Consume the Wing protocol handshake so later recvs see only payload
    {:ok, <<0xDF, 0xD1>>} = :gen_tcp.recv(sock, 2, 1_000)
    :ok = :gen_tcp.controlling_process(sock, owner)
    sock
  end

  test "connecting to a host with no console returns an error tuple instead of panicking" do
    assert {:error, reason} = Wing.connect_with_host("127.0.0.2")
    assert is_binary(reason)
  end

  test "Wing.Console.start_link surfaces a connect failure as an error tuple" do
    assert {:error, {:connection_failed, reason}} = Wing.Console.start_link("127.0.0.2")
    assert is_binary(reason)
  end

  test "one console uses exactly one TCP connection, regardless of subscriptions" do
    with_fake_console!(fn listen ->
      test = self()
      accept_task = Task.async(fn -> accept!(listen, test) end)
      {:ok, console} = Wing.Console.start_link("127.0.0.1")
      sock = Task.await(accept_task)

      # Subscribing to several properties must NOT open more connections —
      # this was the historical leak (one thread + one connection per
      # property, times every reconnect).
      for path <- ["/ch/1/fdr", "/ch/2/fdr", "/ch/3/fdr"] do
        prop_id = Wing.name_to_id(path)
        assert prop_id != -1
        assert :ok = Wing.Console.subscribe_property(console, prop_id, self())
      end

      assert {:error, :timeout} = :gen_tcp.accept(listen, 300)

      # Writes go over the same single connection
      prop_id = Wing.name_to_id("/ch/1/fdr")
      assert :ok = Wing.Console.set_float(console, prop_id, -10.0)
      assert {:ok, data} = :gen_tcp.recv(sock, 0, 1_000)
      assert byte_size(data) > 0

      Wing.Console.stop(console)
    end)
  end

  test "console death is reported to subscribers and the process stops" do
    with_fake_console!(fn listen ->
      test = self()
      accept_task = Task.async(fn -> accept!(listen, test) end)
      {:ok, console} = Wing.Console.start_link("127.0.0.1")
      sock = Task.await(accept_task)

      prop_id = Wing.name_to_id("/ch/1/fdr")
      assert :ok = Wing.Console.subscribe_property(console, prop_id, self())

      ref = Process.monitor(console)

      # The console hangs up (reboot, cable pull, connection-slot kick)
      :gen_tcp.close(sock)

      assert_receive {:wing_connection_lost, _reason}, 5_000
      assert_receive {:DOWN, ^ref, :process, ^console, {:shutdown, :connection_lost}}, 1_000
    end)
  end

  test "stopping the console closes the TCP connection deterministically" do
    with_fake_console!(fn listen ->
      test = self()
      accept_task = Task.async(fn -> accept!(listen, test) end)
      {:ok, console} = Wing.Console.start_link("127.0.0.1")
      sock = Task.await(accept_task)

      assert :ok = Wing.Console.stop(console)

      # stop_reader_thread shuts the socket down — the fake console sees EOF
      # promptly instead of the slot staying occupied until resource GC.
      assert {:error, :closed} = :gen_tcp.recv(sock, 0, 2_000)
    end)
  end
end
