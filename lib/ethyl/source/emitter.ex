defmodule Ethyl.Source.Emitter do
  import GenStage.Utils

  require Logger

  @moduledoc """
  This is a pubsub system as a data structure. It is intended to add
  subscription capability to any process capable of keeping the structure in its
  state, and feed him with subscription and monitor messages.

  This version does not handle "topics", but a single list of listeners,
  although it is possible for a process to maintain several Emitter structures
  in its state.
  """
  alias __MODULE__

  @compile {:inline, send_noconnect: 2}

  # mimicing GenStage for the state
  defstruct [
    # a map to monitor-ref to subscription ref
    monitors: %{},
    # a map to subscription-ref to subscription
    listeners: %{}
  ]

  require Record
  Record.defrecordp(:rsub, :sub, pid: nil, mref: nil)
  import Kernel, except: [lenght: 1]

  def new() do
    %Emitter{}
  end

  def size(%Emitter{listeners: listeners}) do
    Map.size(listeners)
  end

  def subscribe(server, opts \\ [], ref \\ make_ref()) when is_list(opts) do
    msg = {:"$etl_source", {self(), ref}, {:subscribe, opts}}

    case send_to_name(server, msg) do
      :ok -> {:ok, ref}
      {:error, _} = err -> err
    end
  end

  defp send_to_name(server, msg) do
    case GenServer.whereis(server) do
      nil ->
        {:error, :noproc}

      pid ->
        send(pid, msg)
        :ok
    end
  end

  def handle_subscribe(
        %Emitter{
          listeners: listeners
        } = em,
        {:"$etl_source", {listener_pid, ref} = from, {:subscribe, opts}}
      )
      when is_pid(listener_pid) do
    case listeners do
      %{^ref => _} ->
        Logger.error(
          "Source emitter #{inspect(Utils.self_name())} received duplicated subscription from: #{
            from
          }"
        )

        msg = {:"$etl_listener", {self(), ref}, {:cancel, :duplicated_subscription}}
        send_noconnect(listener_pid, msg)
        {:error, :duplicated}

      %{} ->
        mref = Process.monitor(listener_pid)
        em = put_in(em.monitors[mref], ref)

        em =
          put_in(
            em.listeners[ref],
            rsub(pid: listener_pid, mref: mref)
          )

        {:ok, em}
    end
  end

  def emit(%Emitter{listeners: listeners} = em, msg) do
    Enum.map(listeners, fn {ref, rsub(pid: pid)} ->
      send(pid, {:"$etl_listener", {self(), ref}, {:event, msg}})
    end)

    :ok
  end

  def handle_down(%Emitter{monitors: monitors} = em, {:DOWN, mref, :process, pid, _})
      when is_map_key(monitors, mref) do
    {ref, em} = pop_in(em.monitors[mref])
    {rsub(mref: ^mref), em} = pop_in(em.listeners[ref])
    {:ok, em}
  end

  def handle_down(%Emitter{}, {:DOWN, _, _, _, _}) do
    :unknown
  end

  def handle_exit(%Emitter{} = em, {:EXIT, pid, _}) do
    clear_pid(em, pid)
  end

  def clear_pid(%Emitter{listeners: listeners, monitors: monitors} = em, pid) when is_pid(pid) do
    case do_clear_pid(Map.to_list(listeners), pid, {[], []}) do
      {_, []} ->
        :unknown

      {rest_listeners, mrefs} ->
        {:ok, %Emitter{listeners: rest_listeners, monitors: Map.drop(monitors, mrefs)}}
    end
  end

  defp do_clear_pid([{_, rsub(pid: pid, mref: mref)} | subs], pid, {keep, mrefs}) do
    Process.demonitor(mref, [:flush])
    do_clear_pid(subs, pid, {keep, [mref | mrefs]})
  end

  defp do_clear_pid([sub | subs], pid, {keep, mrefs}) do
    do_clear_pid(subs, pid, {[sub | keep], mrefs})
  end

  defp do_clear_pid([], pid, {keep, mrefs}) do
    {Map.new(keep), mrefs}
  end

  defp send_noconnect(pid, msg) do
    Process.send(pid, msg, [:noconnect])
  end
end
