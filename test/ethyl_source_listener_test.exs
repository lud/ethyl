defmodule Ethyl.Source.ListenerTest do
  use ExUnit.Case, async: true
  import Ethyl.TestHelper
  doctest Ethyl.Source.Listener
  alias Ethyl.Source.Listener
  require Logger
  alias Ethyl.MicroFSM

  defmodule MySource do
    def init(arg) do
      Logger.debug("source init")
      {:next, :await_sub, arg}
    end

    def await_sub({:"$etl_source", {pid, ref}, {:subscribe, []}} = msg, _)
        when is_pid(pid) and is_reference(ref) do
      Logger.debug("source received: #{inspect(msg)}")
      {:next, :await_event, {pid, ref}}
    end

    def await_event({:forward, event} = msg, {pid, ref}) do
      Logger.debug("source received: #{inspect(msg)}")
      send(pid, event)
      {:next, :await_event, {pid, ref}}
    end

    def terminate(reason, _) do
      Logger.debug("source terminate: #{inspect(reason)}")
    end
  end

  test "validate the source message API" do
    {:ok, source} = MicroFSM.start_link(MySource, [], debug: [:trace])

    {:ok, listener} = Listener.start_link([source])
    send(source, {:forward, :hello})
    send(listener, :dummy)
    MicroFSM.stop(source)
    # await_down(listener)
  end
end
