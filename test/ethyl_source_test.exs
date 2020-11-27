defmodule Ethyl.SourceTest do
  use ExUnit.Case, async: true
  import Ethyl.TestHelper
  doctest Ethyl.Source

  test "producer assertion test" do
    # We rely on a custom assertion to assert that sources emit expected events
    # so we might as well be sure that our assertion works well
    defmodule Counter do
      use GenStage

      def start_link(number) do
        GenStage.start_link(__MODULE__, number)
      end

      def init(counter) do
        {:producer, counter}
      end

      def handle_demand(demand, counter) when demand > 0 do
        # If the counter is 3 and we ask for 2 items, we will
        # emit the items 3 and 4, and set the state to 5.
        events = Enum.to_list(counter..(counter + demand - 1))
        {:noreply, events, counter + demand}
      end
    end

    # no events
    {:ok, producer} = Counter.start_link(0)
    assert_producer_events(producer, [])
    GenStage.stop(producer)

    # a single event
    {:ok, producer} = Counter.start_link(9999)
    assert_producer_events(producer, [9999])
    GenStage.stop(producer)

    # more events
    {:ok, producer} = Counter.start_link(-1000)
    assert_producer_events(producer, [-1000, -999, -998, -997])
    GenStage.stop(producer)
  end

  test "listen to a pubsub event" do
    topic = :my_topic
    {:ok, ps} = Ethyl.PubSub.start_link()
    {:ok, source} = Ethyl.PubSub.EventSource.start_link(pubsub: ps, topic: topic)

    Ethyl.PubSub.publish(ps, topic, "these")
    Ethyl.PubSub.publish(ps, topic, "are")

    spawn(fn ->
      Process.sleep(100)
      Ethyl.PubSub.publish(ps, topic, "some")
      Ethyl.PubSub.publish(ps, topic, "words")
    end)

    assert_producer_events(source, ~w(these are some words))
  end
end
