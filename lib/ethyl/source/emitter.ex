defmodule Ethyl.Source.Emitter do
  @moduledoc """
  This is a pubsub system as a data structure. It is intended to add
  subscription capability to any process capable of keeping the structure in its
  state, and feed him with subscription and monitor messages.

  This version does not handle "topics", but a single list of listeners,
  although it is possible for a process to maintain several Emitter structures
  in its state.
  """
  alias __MODULE__
  defstruct subs: []

  require Record
  Record.defrecordp(:rsub, :sub, pid: nil, ref: nil)
  import Kernel, except: [lenght: 1]

  def new() do
    %Emitter{}
  end

  def length(%Emitter{subs: subs}) do
    Kernel.length(subs)
  end

  def subscribe(%Emitter{subs: subs} = em, pid) when is_pid(pid) do
    ref = Process.monitor(pid)
    sub = rsub(pid: pid, ref: ref)
    {:ok, %Emitter{em | subs: [sub | subs]}}
  end

  def emit(%Emitter{subs: subs} = em, msg) do
    Enum.map(subs, fn rsub(pid: pid) -> send(pid, msg) end)
    :ok
  end

  def handle_down(%Emitter{subs: subs} = em, {:DOWN, ref, :process, pid, _}) do
    case remove_ref(subs, ref, []) do
      {:ok, subs} -> {:ok, %Emitter{em | subs: subs}}
      :unknown -> :unknown
    end
  end

  def handle_down(%Emitter{}, {:DOWN, _, _, _, _}) do
    :unknown
  end

  defp remove_ref([rsub(ref: ref) | rest], ref, acc) do
    # note, we do not care about the order of subscriptions, so we do not try
    # to preserve it while iterating over the lists.
    {:ok, rest ++ acc}
  end

  defp remove_ref([other | rest], ref, acc) do
    remove_ref(rest, ref, [other | acc])
  end

  defp remove_ref([], _, _) do
    :unknown
  end

  def handle_exit(%Emitter{} = em, {:EXIT, pid, _}) do
    clear_pid(em, pid)
  end

  def clear_pid(%Emitter{subs: subs} = em, pid) when is_pid(pid) do
    case do_clear_pid(subs, pid, {[], 0}) do
      {:ok, subs} -> {:ok, %Emitter{em | subs: subs}}
      :unknown -> :unknown
    end
  end

  defp do_clear_pid([rsub(pid: pid, ref: ref) | subs], pid, {acc, count}) do
    Process.demonitor(ref, [:flush])
    do_clear_pid(subs, pid, {acc, count + 1})
  end

  defp do_clear_pid([sub | subs], pid, {acc, count}) do
    do_clear_pid(subs, pid, {[sub | acc], count})
  end

  defp do_clear_pid([], pid, {_, 0}) do
    :unknown
  end

  defp do_clear_pid([], _pid, {acc, _count}) do
    {:ok, acc}
  end
end
