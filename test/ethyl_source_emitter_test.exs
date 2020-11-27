defmodule Ethyl.Source.EmitterTest do
  use ExUnit.Case
  import Ethyl.TestHelper
  doctest Ethyl.Source.Emitter
  alias Ethyl.Source.Emitter

  test "can subscribe and publish" do
    em = Emitter.new()
    assert {:ok, em} = Emitter.subscribe(em, self())
    assert 1 = Emitter.length(em)
    assert :ok = Emitter.emit(em, :hello)
    assert_receive :hello
  end

  test "will set monitors and can handle them" do
    em = Emitter.new()
    parent = self()

    child =
      spawn(fn ->
        receive do
          msg -> send(parent, {:child_received, msg})
        end
      end)

    assert {:ok, em} = Emitter.subscribe(em, child)
    assert 1 = Emitter.length(em)
    assert :ok = Emitter.emit(em, :some_msg)
    refute_receive :some_msg
    assert_receive {:child_received, :some_msg}
    downmsg = assert_receive {:DOWN, _ref, :process, ^child, :normal}

    # The emitter doesn't know about the down message yet so the size is still 1
    assert 1 = Emitter.length(em)

    # Now it will remove the subscription
    assert {:ok, em} = Emitter.handle_down(em, downmsg)
    assert 0 = Emitter.length(em)

    # It will not handle unknown :DOWN messages
    {other_pid, ref} = spawn_monitor(fn -> :ok end)
    downmsg = assert_receive {:DOWN, ^ref, :process, ^other_pid, :normal}
    assert :unknown = Emitter.handle_down(em, downmsg)
  end

  test "it can handle exit messages" do
    prev_trap_exit = Process.flag(:trap_exit, true)

    em = Emitter.new()
    parent = self()

    # Handling exit for a known child
    child = spawn_link(fn -> Process.sleep(:infinity) end)
    assert {:ok, em} = Emitter.subscribe(em, child)
    assert 1 = Emitter.length(em)
    assert Process.alive?(child)
    Process.exit(child, :byebye)
    await_down(child)
    exitmsg = assert_receive {:EXIT, ^child, :byebye}

    # The emitter can handle the exit message
    assert {:ok, em} = Emitter.handle_exit(em, exitmsg)
    assert 0 = Emitter.length(em)
    # The down message has been flushed
    refute_receive {:DOWN, _ref, :process, _, _}

    # Handling unknown exit messages - not subscribed
    other_child = spawn_link(fn -> Process.sleep(:infinity) end)
    assert Process.alive?(other_child)
    Process.exit(other_child, :byebye)
    await_down(other_child)
    exitmsg = assert_receive {:EXIT, ^other_child, :byebye}
    assert :unknown = Emitter.handle_exit(em, exitmsg)
    Process.flag(:trap_exit, prev_trap_exit)
  end
end
