defmodule Ethyl.Source.ListenerTest do
  use ExUnit.Case, async: true
  doctest Ethyl.Source.GenStageProducerListener

  alias Ethyl.MicroFSM
  alias Ethyl.Source.Emitter
  alias Ethyl.Source.GenStageProducerListener
  import Ethyl.TestHelper
  require Logger

  defmodule MySource do
    def init(arg) do
      Logger.debug("source init")
      {:next, :await_sub, Emitter.new()}
    end

    def await_sub({:"$etl_source", {pid, ref}, {:subscribe, []}} = msg, em)
        when is_pid(pid) and is_reference(ref) do
      Logger.debug("source received: #{inspect(msg)}")
      {:ok, em} = Emitter.handle_subscribe(em, msg)
      {:next, :await_event, em}
    end

    def await_event({:forward, event} = msg, em) do
      Logger.debug("source received: #{inspect(msg)}")
      :ok = Emitter.emit(em, event)
      {:next, :await_event, em}
    end

    def await_event(msg, em) do
      Logger.error("source received unexpected message: #{inspect(msg)}")
      {:next, :await_event, em}
    end

    def terminate(reason, _) do
      Logger.debug("source terminate: #{inspect(reason)}")
    end
  end

  test "validate the source message API" do
    {:ok, source} = MicroFSM.start(MySource, [])

    {:ok, listener} = GenStageProducerListener.start([{source, []}])
    send(source, {:forward, :hello})
    send(source, {:forward, :i})
    send(source, {:forward, :am})
    send(source, {:forward, :lud})
    assert_producer_events(listener, ~w(hello i am lud)a)

    # By default the genstage producer (listener) will exit if its source exits
    ref = Process.monitor(listener)
    MicroFSM.stop(source, :failure)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
