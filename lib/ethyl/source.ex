defmodule Ethyl.Source do
  require Ethyl.Utils, as: Utils
  alias Ethyl.Opts

  def subscribe({server, opts}) when Utils.is_name(server) and is_list(opts) do
    do_subscribe(server, opts, make_ref())
  end

  def subscribe(server, opts) when Utils.is_name(server) and is_list(opts) do
    do_subscribe(server, opts, make_ref())
  end

  def subscribe(server, opts, ref) do
    case Opts.resolve_name(server) do
      {:ok, pid} -> do_subscribe(pid, opts, ref)
      {:error, :noproc} -> {:error, {:source_dead, server}}
    end
  end

  def subscribe_monitor({server, opts}) when Utils.is_name(server) and is_list(opts) do
    subscribe_monitor(server, opts)
  end

  def subscribe_monitor(server, opts) when Utils.is_name(server) and is_list(opts) do
    case Opts.resolve_name(server) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        do_subscribe(pid, opts, ref)

      {:error, :noproc} ->
        {:error, {:source_dead, server}}
    end
  end

  defp do_subscribe(pid, opts, ref) do
    msg = {:"$etl_source", {self(), ref}, {:subscribe, opts}}
    send(pid, msg)
    {:ok, ref}
  end
end
