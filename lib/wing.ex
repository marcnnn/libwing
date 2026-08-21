defmodule Wing do
  use Rustler, otp_app: :libwing, crate: "wing"

  def connect(), do: :erlang.nif_error(:nif_not_loaded)
  def connect_with_host(_host), do: :erlang.nif_error(:nif_not_loaded)
  def scan(), do: :erlang.nif_error(:nif_not_loaded)
  def read(_pid), do: :erlang.nif_error(:nif_not_loaded)
  def read_simple(_pid), do: :erlang.nif_error(:nif_not_loaded)
  def start_meter_thread(_console_ref, _pid, _meters), do: :erlang.nif_error(:nif_not_loaded)
  def update_meter_thread(_meter_handle, _meters), do: :erlang.nif_error(:nif_not_loaded)
  def stop_meter_thread(_meter_handle), do: :erlang.nif_error(:nif_not_loaded)
  def name_to_id(_name), do: :erlang.nif_error(:nif_not_loaded)
  def set_float(_pid, _id, _value), do: :erlang.nif_error(:nif_not_loaded)
  def request_node_data(_pid, _id), do: :erlang.nif_error(:nif_not_loaded)
  def start_reader_thread(_console_ref, _pid), do: :erlang.nif_error(:nif_not_loaded)
  def stop_reader_thread(_reader_handle), do: :erlang.nif_error(:nif_not_loaded)

  @doc deprecated:
         "Spawns an unstoppable thread with its own TCP connection per property. " <>
           "Use Wing.Console with start_reader_thread/2 instead."
  def start_property_thread(_host, _pid, _prop_id), do: :erlang.nif_error(:nif_not_loaded)
  def init_wing_thread(_host), do: :erlang.nif_error(:nif_not_loaded)
end
