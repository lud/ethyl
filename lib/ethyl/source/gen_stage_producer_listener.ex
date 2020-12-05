defmodule Ethyl.Source.GenStageProducerListener do
  use GenStage
  require Logger
  alias Ethyl.Source
  alias Ethyl.Source.Emitter
  require Ethyl.Utils, as: Utils

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
    do_start(:start_link, sources, opts)
  end

  def start(sources, opts \\ []) do
    do_start(:start, sources, opts)
  end

  defp do_start(fun, sources, opts) do
    case validate_sources(sources) do
      {:ok, sources} -> apply(GenStage, fun, [__MODULE__, sources, opts])
      {:error, _} = err -> err
    end
  end

  defp validate_sources(sources) when not is_list(sources) do
    validate_sources(List.wrap(sources))
  end

  defp validate_sources(sources) when is_list(sources) do
    sources
    |> Enum.all?(fn
      {source, opts} when Utils.is_name(source) and is_list(opts) -> true
      _ -> false
    end)
    |> if do
      {:ok, sources}
    else
      {:error, {:bad_sources, sources}}
    end
  end

  def init(sources) do
    case subscribe_all(sources, []) do
      {:ok, subs} ->
        state = %State{subs: subs, queue: :queue.new(), pending: 0}
        {:producer, state, dispatcher: GenStage.BroadcastDispatcher}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp subscribe_all([source | sources], acc) do
    case send_subscription(source) do
      {:ok, subscription} -> subscribe_all(sources, [subscription | acc])
      {:error, _} = err -> err
    end
  end

  defp subscribe_all([], acc) do
    {:ok, Map.new(acc)}
  end

  def send_subscription({name, opts}) do
    with {:ok, pid} <- Ethyl.Opts.resolve_name(name),
         {:ok, ref} <- Source.subscribe_monitor(pid, opts) do
      # return a key/value tuple to store all subscriptions in a map
      # in the value we keep a tag and a pid.
      _sub_kv = {:ok, {ref, {:default, pid}}}
    end
  end

  def handle_info({:"$etl_listener", _from, {:data, event}}, state) do
    queue = :queue.in(event, state.queue)
    dispatch_events(queue, state.pending, [], state)
  end

  def handle_info({:DOWN, ref, _, _, reason}, state) when is_map_key(state.subs, ref) do
    Logger.debug("received :DOWN message in #{__MODULE__} with reason: #{inspect(reason)}")
    {:stop, reason, state}
  end

  def handle_info(msg, state) do
    Logger.error("received unexpected message in #{__MODULE__}: #{inspect(msg)}")
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
