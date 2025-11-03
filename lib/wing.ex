defmodule Wing do
  use Rustler, otp_app: :libwing, crate: "wing"

  # Core connection NIFs
  def connect(), do: :erlang.nif_error(:nif_not_loaded)
  def connect_with_host(_host), do: :erlang.nif_error(:nif_not_loaded)
  def scan(), do: :erlang.nif_error(:nif_not_loaded)
  def read(_wing_arc), do: :erlang.nif_error(:nif_not_loaded)
  def read_simple(_wing_arc), do: :erlang.nif_error(:nif_not_loaded)
  
  # Arc-based thread NIFs (used by Wing.Console)
  def start_meter_thread_arc(_wing_arc, _pid, _meters), do: :erlang.nif_error(:nif_not_loaded)
  def start_property_thread_arc(_wing_arc, _pid, _prop_id), do: :erlang.nif_error(:nif_not_loaded)
  def start_unified_property_thread(_wing_arc, _pid), do: :erlang.nif_error(:nif_not_loaded)
  
  # Utility NIFs
  def name_to_id(_name), do: :erlang.nif_error(:nif_not_loaded)
  def set_float(_wing_arc, _id, _value), do: :erlang.nif_error(:nif_not_loaded)
  def request_node_data(_wing_arc, _id), do: :erlang.nif_error(:nif_not_loaded)
  def init_wing_thread(_host), do: :erlang.nif_error(:nif_not_loaded)
end
