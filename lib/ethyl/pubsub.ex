defmodule Ethyl.PubSub do
  use GenServer

  require Logger

  require Record
  Record.defrecordp(:rsub, :sub, pid: nil, tag: nil)
  Record.defrecordp(:rcnfo, :cinf, topic: nil, tag: nil)

  @cleanup_timeout 10_000

  def subscribe(ps, topic, opts \\ []) do
    subscribe_from(ps, self(), topic, opts)
  end

  def subscribe_from(ps, pid, topic, opts \\ []) when is_pid(pid) do
    GenServer.call(ps, {:subscribe, pid, topic, opts})
  end

  def unsubscribe(ps, topic, opts \\ []) do
    GenServer.call(ps, {:unsubscribe, self(), topic, opts})
  end

  def clear(ps, pid \\ self()) do
    GenServer.call(ps, {:clear, pid})
  end

  def publish(ps, topic, value) do
    GenServer.call(ps, {:publish, topic, value})
  end

  def link(ps) when is_pid(ps) do
    Process.link(ps)
  end

  def link(ps) do
    GenServer.call(ps, {:link, self()})
  end

  # -- Pub sub server implementation ------------------------------------------

  def start_link(otp_opts \\ []) do
    GenServer.start_link(__MODULE__, [], otp_opts)
  end

  def start(otp_opts \\ []) do
    GenServer.start(__MODULE__, [], otp_opts)
  end

  @impl GenServer
  def init(_) do
    # We will trap exits so clients can be linked to us (on demand) and exit
    # if their source of events (us) goes down.
    Process.flag(:trap_exit, true)
    schedule_cleanup()
    {:ok, %{topic2subs: %{}, clients: %{}}}
  end

  @impl GenServer
  def handle_call({:subscribe, client, topic, opts}, from, state) do
    GenServer.reply(from, :ok)

    if opts[:link] do
      Process.link(client)
    end

    # Create the subscription data
    sub = rsub(pid: client, tag: opts[:tag])

    state = add_subscription(state, topic, sub)

    {:noreply, state}
  end

  def handle_call({:publish, topic, value}, from, state) do
    GenServer.reply(from, :ok)

    state.topic2subs
    |> Map.get(topic, [])
    |> Enum.map(fn sub -> send_event(sub, value) end)

    {:noreply, state}
  end

  def handle_call({:unsubscribe, pid, topic, opts}, from, state) do
    GenServer.reply(from, :ok)

    state = delete_subcription(state, pid, topic, opts[:tag])

    {:noreply, state}
  end

  def handle_call({:clear, pid}, from, state) do
    GenServer.reply(from, :ok)
    {:noreply, clear_pid(state, pid)}
  end

  def handle_call({:link, pid}, _from, state) do
    Process.link(pid)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({:EXIT, pid, _}, state) do
    {:noreply, clear_pid(state, pid)}
  end

  def handle_info(:cleanup, state) do
    schedule_cleanup()
    {:noreply, cleanup(state), :infinity}
  end

  defp send_event(rsub(pid: pid, tag: tag), value) do
    case tag do
      nil -> send(pid, value)
      tag -> send(pid, {tag, value})
    end
  end

  defp add_subscription(state, topic, sub) do
    rsub(pid: pid, tag: tag) = sub
    client_info = rcnfo(topic: topic, tag: tag)

    state
    # store the sub (pid+tag) under the topic
    |> update_in([:topic2subs, Access.key(topic, [])], &[sub | &1 -- [sub]])
    # store the topic+tag under the pid
    |> update_in([:clients, Access.key(pid, [])], &[client_info | &1 -- [client_info]])
  end

  defp delete_subcription(state, pid, topic, tag) do
    sub = rsub(pid: pid, tag: tag)
    client_info = rcnfo(topic: topic, tag: tag)

    state =
      state
      # Delete the subscription from the topic
      |> update_in([:topic2subs, Access.key(topic, [])], &(&1 -- [sub]))
      # Delete the client_info from the client
      |> update_in([:clients, Access.key(pid, [])], &(&1 -- [client_info]))

    # If there is no more subscription for this client we can unlink it
    case state.clients[pid] do
      [] -> Process.unlink(pid)
      _ -> :ok
    end

    state
  end

  defp clear_pid(state, pid) do
    Process.unlink(pid)

    client_infos = Map.get(state.clients, pid, [])

    # topic2subs =
    #   for rcnfo(topic: client_topic, tag: tag) <- Map.get(state.clients, pid, []),
    #       reduce: state.topic2subs do
    #     t2s -> Map.update!(t2s, client_topic, &(&1 -- [rsub(pid: pid, tag: tag)]))
    #   end
    topic2subs =
      Enum.reduce(
        client_infos,
        state.topic2subs,
        fn rcnfo(topic: client_topic, tag: tag), t2s ->
          Map.update!(t2s, client_topic, &(&1 -- [rsub(pid: pid, tag: tag)]))
        end
      )

    %{state | topic2subs: topic2subs}
  end

  defp schedule_cleanup() do
    Process.send_after(self(), :cleanup, @cleanup_timeout)
  end

  defp cleanup(state) do
    %{
      state
      | topic2subs: remove_empty_lists(state.topic2subs),
        clients: remove_empty_lists(state.clients)
    }
  end

  defp remove_empty_lists(map) when is_map(map) do
    Enum.filter(map, fn
      {_, []} -> false
      _ -> true
    end)
    |> Enum.into(%{})
  end
end
