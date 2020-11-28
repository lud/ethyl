ExUnit.start()

defmodule Ethyl.TestHelper do
  import ExUnit.Assertions

  defmodule TestConsumer do
    use GenStage

    def start_link(producer, parent, ref) when is_pid(parent) do
      GenStage.start_link(__MODULE__, [producer, parent, ref])
    end

    def init([producer, parent, ref]) do
      {:consumer, {parent, ref}, subscribe_to: [producer]}
    end

    def handle_events(events, _from, {parent, ref} = state) do
      send(parent, {ref, :events, events})
      {:noreply, [], state}
    end
  end

  # Asserts that the producer will emit the expected events, in order. If more
  # events are emitted the assertion is still valid.
  def assert_producer_events(producer, expected, timeout \\ 5000) when is_pid(producer) do
    # @todo timeout is repeated whenever we receive a message. We should use a
    #     task to subscribe
    ref = make_ref()
    {:ok, consumer} = TestConsumer.start_link(producer, self(), ref)
    mref = Process.monitor(consumer)

    case assert_producer_events_loop(ref, mref, expected, timeout) do
      :done ->
        assert true
        GenStage.stop(consumer, :normal, 5000)

      {:failed, expected, other} ->
        GenStage.stop(consumer, :normal, 5000)

        flunk("""
        Expected event: #{inspect(expected)}
        Got: #{inspect(other)}
        """)

      :timeout ->
        flunk("Expected event but got timeout")
    end
  end

  defp assert_producer_events_loop(ref, mref, expected, timeout) do
    receive do
      {^ref, :events, events} ->
        case assert_lists_common_root(expected, events) do
          {:continue, rest} -> assert_producer_events_loop(ref, mref, rest, timeout)
          termination -> termination
        end
    after
      timeout -> :timeout
    end
  end

  defp assert_lists_common_root(expected, found)

  defp assert_lists_common_root([h | a], [h | b]) do
    assert_lists_common_root(a, b)
  end

  defp assert_lists_common_root([h | []], [h | _]) do
    # expected ended, we have more in the results list but it is ok
    :done
  end

  defp assert_lists_common_root([], _) do
    :done
  end

  defp assert_lists_common_root([_ | _] = rest, []) do
    # expected ended, we have more in the results list but it is ok
    {:continue, rest}
  end

  defp assert_lists_common_root([h | _], [other | _]) do
    # expected ended, we have more in the results list but it is ok

    {:failed, h, other}
  end

  @spec simple_child((() -> state), (state, next :: (state -> no_return) -> no_return)) :: pid
        when state: any
  def simple_child(init, loop) when is_function(init, 0) and is_function(loop, 2) do
    parent = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        state = init.()
        send(parent, {:ack, ref})
        simple_child_loop(state, loop)
      end)

    receive do
      {:ack, ^ref} -> pid
    after
      1000 -> exit(:could_not_start_simple_child)
    end
  end

  defp simple_child_loop(state, loop) do
    loop.(state, fn state -> simple_child_loop(state, loop) end)
  end

  def kill_sync(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      1000 -> exit({:could_not_kill_sync, pid})
    end
  end

  def await_down(pid, timeout \\ 1000) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      timeout -> exit({:process_is_not_DOWN, pid})
    end
  end
end
