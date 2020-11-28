defmodule Ethyl.MicroFSM do
  use GenServer

  def start_link(mod, arg, opts \\ []) do
    GenServer.start_link(__MODULE__, {mod, arg}, opts)
  end

  def start(mod, arg, opts \\ []) do
    GenServer.start(__MODULE__, {mod, arg}, opts)
  end

  def stop(pid, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(pid, reason, timeout)
  end

  def init({mod, arg}) do
    case mod.init(arg) do
      {:next, fun, state} -> {:ok, {mod, fun, state}}
      {:stop, _} = stop -> stop
      other -> {:stop, {:bad_return, other}}
    end
  end

  def handle_info(msg, {mod, fun, state}) do
    handle_result(apply(mod, fun, [msg, state]), mod)
  end

  defp handle_result({:next, fun, state}, mod) do
    {:noreply, {mod, fun, state}}
  end

  defp handle_result({:stop, reason, state}, mod) do
    {:stop, reason, {mod, nil, state}}
  end

  def terminate(reason, {mod, _, state}) do
    if function_exported?(mod, :terminate, 2) do
      mod.terminate(reason, state)
    else
      :ok
    end
  end
end
