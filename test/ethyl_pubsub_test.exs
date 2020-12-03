defmodule Ethyl.PubSubTest do
  use ExUnit.Case, async: true
  doctest Ethyl.PubSub
  import Ethyl.TestHelper

  alias Ethyl.PubSub

  test "can subscribe, publish and receive a message" do
    {:ok, ps} = PubSub.start_link()
    assert :ok = PubSub.subscribe(ps, :topic_1)
    assert :ok = PubSub.publish(ps, :topic_1, :hello)
    assert_receive :hello

    # subscribe with the same options
    :ok = PubSub.subscribe(ps, :topic_1)
    :ok = PubSub.publish(ps, :topic_1, :hello)
    # we will receive the message only once
    assert_receive :hello
    refute_receive :hello

    # subscribe with a wrapper tag
    :ok = PubSub.subscribe(ps, :topic_1, tag: :pubsub)
    :ok = PubSub.publish(ps, :topic_1, :hello)
    assert_receive :hello
    refute_receive :hello
    assert_receive {:pubsub, :hello}

    # a nil tag is like no tag
    :ok = PubSub.subscribe(ps, :topic_nil, tag: nil)
    :ok = PubSub.publish(ps, :topic_nil, :hola)
    assert_receive :hola
    refute_receive {nil, :hola}

    # clear the subscriptions
    assert :ok = PubSub.clear(ps)
    :ok = PubSub.publish(ps, :topic_1, :hello)
    refute_receive :hello
    refute_receive {:pubsub, :hello}

    # Obviously we should not receive any message for a different topic, even if
    # we use our subscribed topic as a tag
    :ok = PubSub.subscribe(ps, :OTHER_TOPIC, tag: :pubsub)
    :ok = PubSub.subscribe(ps, :OTHER_TOPIC, tag: :pubsub)
    :ok = PubSub.publish(ps, :topic_1, :hello)
    refute_receive :hello
    refute_receive {:pubsub, :hello}

    PubSub.clear(ps)
  end

  test "deffered subscription" do
    {:ok, ps} = PubSub.start()

    parent = self()

    child =
      simple_child(
        fn -> nil end,
        # we will keep the last value as state
        fn state, next ->
          receive do
            msg ->
              send(parent, {self(), :received, msg})
              next.(state)
          end
        end
      )

    # No tag
    # note, we link the child to the pubsub
    assert :ok = PubSub.subscribe_from(ps, child, :a_topic, link: true)
    assert :ok = PubSub.publish(ps, :a_topic, :some_val)
    assert_receive {^child, :received, :some_val}
    refute_receive :some_val

    # tagged
    assert :ok = PubSub.subscribe_from(ps, child, :a_topic, tag: :wrapped, link: true)
    assert :ok = PubSub.publish(ps, :a_topic, :some_val_2)
    assert_receive {^child, :received, {:wrapped, :some_val_2}}
    assert_receive {^child, :received, :some_val_2}
    refute_receive :some_val_2
    refute_receive {:wrapped, :some_val_2}

    kill_sync(ps)
    assert :ok = await_down(child)
  end

  test "cannot have duplicate subscriptions" do
    topic = :dupes
    {:ok, ps} = PubSub.start_link()
    assert :ok = PubSub.subscribe(ps, topic, tag: :tag_dup)
    assert :ok = PubSub.subscribe(ps, topic, tag: :tag_dup)
    assert :ok = PubSub.subscribe(ps, topic, tag: :other)

    PubSub.publish(ps, topic, :hello)

    # Receive tagged with :tag_dup only once
    assert_receive {:tag_dup, :hello}
    refute_receive {:tag_dup, :hello}

    # Still receive tagged with :other tag
    assert_receive {:other, :hello}

    PubSub.clear(ps)
  end

  test "unsubscribe" do
    topic = :unsub
    {:ok, ps} = PubSub.start_link()
    assert :ok = PubSub.subscribe(ps, topic, tag: :tag_1)
    assert :ok = PubSub.subscribe(ps, topic, tag: :tag_2)
    assert :ok = PubSub.subscribe(ps, topic)

    PubSub.publish(ps, topic, :hello)
    assert_receive {:tag_1, :hello}
    assert_receive {:tag_2, :hello}
    assert_receive :hello

    # unsubscribe with tag
    assert :ok = PubSub.unsubscribe(ps, topic, tag: :tag_1)
    PubSub.publish(ps, topic, :hello)
    assert_receive {:tag_2, :hello}
    assert_receive :hello
    refute_receive {:tag_1, :hello}

    # unsubscribe the default tag
    assert :ok = PubSub.unsubscribe(ps, topic)
    assert :ok = PubSub.publish(ps, topic, :hi!)
    assert_receive {:tag_2, :hi!}
    refute_receive :hi!

    PubSub.clear(ps)
  end

  test "can clear process subscriptions" do
    topic = :clearable
    {:ok, ps} = PubSub.start_link()
    assert :ok = PubSub.subscribe(ps, topic)
    assert :ok = PubSub.publish(ps, topic, :hello)
    assert_receive :hello

    PubSub.clear(ps)
    assert :ok = PubSub.publish(ps, topic, :hi!)
    refute_receive :hi!
  end

  test "traping exits and linking processes" do
    # We will test that when subscribing with the link option, if the PubSub
    # server terminates, a linked process will terminate (at least if it is not)
    # trapping exits. But if the subscriber terminates, the PubSub server will
    # stay alive (as it is trapping exits).
    {:ok, ps} = PubSub.start()
    topic = :killer_topic

    start_child = fn ->
      simple_child(
        fn ->
          PubSub.subscribe(ps, topic, tag: :pubsub, link: true)
          nil
        end,
        # we will keep the last value as state
        fn state, next ->
          receive do
            {:pubsub, value} ->
              next.(value)

            {:get_last, from} ->
              send(from, {:last, state})
              next.(state)
          end
        end
      )
    end

    child1 = start_child.()
    PubSub.publish(ps, topic, :hello)
    send(child1, {:get_last, self()})
    assert_receive {:last, :hello}
    # Kill the child. The PS server must still be alive
    kill_sync(child1)
    refute Process.alive?(child1)
    assert Process.alive?(ps)

    child2 = start_child.()
    PubSub.publish(ps, topic, :hi!)
    send(child2, {:get_last, self()})
    assert_receive {:last, :hi!}
    # Kill the child. The PS server must still be alive.
    # We will kill it from a spawned process and not the test process which is
    # the ancestor (this is a special case)
    killer = spawn(fn -> kill_sync(ps) end)
    # await the killer
    assert :ok = await_down(killer)
    refute Process.alive?(child2)
    refute Process.alive?(ps)
  end
end
