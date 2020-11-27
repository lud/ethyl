defmodule Ethyl.Source do
  use GenStage

  # TODO.todo("""
  # Source as it is now is useless: we still need to implement stuf on the
  # webhooks, stores, cron-emitter processes.

  # And that would be a message API.

  # So we should just implement a non-behaviour, non-macro GenStage producer that
  # would be a "SourceListener". It would communicate with processes implementing
  # the Source message API (not even a behaviour).

  # The Source module would then provide an functional pubsub datastructure to
  # handle the incoming messages (keep track of subscribers), publish events, and
  # handle monitors (an optionnaly exits).

  # Then we would have a mimic of a GenStage producer. We could just implement a
  # producer message API but with our own custom protocol we can just ignore
  # back-pressure handling (as we have only push events anyway) and leave the
  # possibility to actually implement the producer protocol for other goals. 
  # """)

  @moduledoc """
  Implements a GenStage producer that must be `use`ed.

  ### GenStage specifics

  The GenStage producer handles buffering of demands and buffering of events as
  described
  [here](https://hexdocs.pm/gen_stage/GenStage.html#module-buffering-demand).

  The dispatcher is a `GenStage.BroadcastDispatcher`.
  """

  # IMPORTANT, We are basically mimicing a mini-producer here, with our own
  # buffering of events. But this is ok as we also buffer demands, and let
  # GenStage keep the event buffering for fault-tolerance purposes. Also it
  # allows us to define a new behaviour that will not interfere with the
  # GenStage callbacks. Notably, it lets us have full control over
  # handle_info/2.

  @type state :: any
  @callback subscribe(init_arg :: any) ::
              {:ok, state}
              | :ignore
              | {:stop, reason :: any}
  @callback handle_msg(msg :: any, state) ::
              {:events, list(any), state}
              | {:ignore, state}
              | {:stop, reason :: any, state}

  @callback terminate(reason :: any, state) :: any

  @optional_callbacks [terminate: 2]
  defmacro __using__(_) do
    quote location: :keep, bind_quoted: [] do
      # This is a simple extension of a gen stage producer with a default
      # implementation
      @behaviour Ethyl.Source
    end
  end

  defmodule State do
    defstruct [
      # Callback module
      :mod,
      # Callback module state
      :sub,
      # Buffer for events
      :queue,
      # Pending demand count
      :pending
    ]
  end

  def start_link(mod, arg, gen_opts \\ []) do
    GenStage.start_link(__MODULE__, {mod, arg}, gen_opts)
  end

  def start(mod, arg, gen_opts \\ []) do
    GenStage.start(__MODULE__, {mod, arg}, gen_opts)
  end

  def init({mod, arg}) do
    case mod.subscribe(arg) do
      {:ok, sub} ->
        init_source(mod, sub)

      :ignore ->
        :ignore

      {:stop, _} = stop ->
        stop

      other ->
        {:stop, {:bad_return_value, other}}
    end
  end

  def init_source(mod, sub) do
    state = %State{
      mod: mod,
      sub: sub,
      queue: :queue.new(),
      pending: 0
    }

    {:producer, state, dispatcher: GenStage.BroadcastDispatcher}
  end

  def handle_info(msg, state) do
    %{queue: queue, mod: mod, sub: sub, pending: pending} = state

    case mod.handle_msg(msg, sub) do
      {:events, events, new_sub} when is_list(events) ->
        queue =
          case events do
            [single] -> :queue.in(single, queue)
            more -> :queue.join(queue, :queue.from_list(more))
          end

        dispatch_events(queue, pending, [], %State{state | sub: new_sub})

      {:ignore, sub} ->
        {:noreply, [], state}

      {:stop, reason, new_sub} ->
        {:stop, reason, %State{state | sub: new_sub}}
    end
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
  def terminate(reason, %{mod: mod, sub: sub}) do
    if function_exported?(mod, :terminate, 2) do
      mod.terminate(reason, sub)
    else
      :ok
    end
  end
end
