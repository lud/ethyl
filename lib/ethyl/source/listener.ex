defmodule Ethyl.Source.Listener do
  use GenStage
  require Logger

  @todo "Allow multiple, uniquely tagged sources"
  @todo """
  Maybe listener should be renamed to entrypoint and :"$etl_entrypoint" to make
  obvious that source/listeners are used at the beginning of a data pipeline, 
  whereas the rest of the line is GenStage based.
  """

  @moduledoc """

  Implements a GenStage producer that subscribes to a source. The goal is to
  switch protocols between GenStage and a push based system. Sources are pushing
  events whereas GenStage is pull-based.

  ### GenStage specifics

  The GenStage producer handles buffering of demands and buffering of events as
  described
  [here](https://hexdocs.pm/gen_stage/GenStage.html#module-buffering-demand).

  The dispatcher is a `GenStage.BroadcastDispatcher`.
  """

  defmodule State do
    defstruct [
      # Buffer for events
      :queue,
      # Pending demand count
      :pending,
      # Sources subscriptions
      :subs
    ]
  end

  def start_link(sources, opts \\ []) do
    sources = List.wrap(sources)
    GenStage.start_link(__MODULE__, sources, opts)
  end

  def start(sources, opts \\ []) do
    sources = List.wrap(sources)
    GenStage.start(__MODULE__, sources, opts)
  end

  def init(sources) do
    case subscribe(sources, []) do
      {:ok, subs} ->
        state = %State{subs: subs, queue: :queue.new(), pending: 0}
        {:producer, state, dispatcher: GenStage.BroadcastDispatcher}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp subscribe([source | sources], acc) do
    case send_subscribe(source) do
      {:ok, subscription} -> subscribe(sources, [subscription | acc])
      {:error, _} = err -> err
    end
  end

  defp subscribe([], acc) do
    {:ok, Map.new(acc)}
  end

  def send_subscribe(pid) when is_pid(pid) do
    ref = Process.monitor(pid)
    Logger.debug("set monitor from #{__MODULE__}: #{inspect(ref)}")
    msg = {:"$etl_source", {self(), ref}, {:subscribe, []}}
    send(pid, msg)
    # @todo set the source tag instead of :default if defined
    {:ok, {ref, {:default, pid}}}
  end

  def send_subscribe(source) do
    case GenServer.whereis(source) do
      nil ->
        Logger.debug("No process found for source: #{inspect(source)}")
        {:error, :noproc}

      pid ->
        send_subscribe(pid)
    end
  end

  def handle_info(msg, state) do
    Logger.debug("received msg in #{__MODULE__}: #{inspect(msg)}")
    {:noreply, [], state}
  end

  def handle_demand(incoming_demand, state) do
    dispatch_events(state.queue, incoming_demand + state.pending, [], state)
  end

  defp dispatch_events(queue, 0, events, state) do
    {:noreply, Enum.reverse(events), %State{state | queue: queue, pending: 0}}
  end

  defp dispatch_events(queue, demand, events, state) do
    case :queue.out(queue) do
      {{:value, event}, queue} ->
        dispatch_events(queue, demand - 1, [event | events], state)

      {:empty, queue} ->
        {:noreply, Enum.reverse(events), %State{state | queue: queue, pending: demand}}
    end
  end

  @doc false
  def terminate(reason, _state) do
    Logger.debug("#{__MODULE__} terminate: #{inspect(reason)}")
  end
end
