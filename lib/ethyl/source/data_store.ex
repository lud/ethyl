defmodule Ethyl.Source.DataStore do
  use GenServer
  alias Ethyl.Opts
  require Ethyl.Utils, as: Utils
  alias Ethyl.Source
  require Logger

  @callback init(term) :: state when state: term
  @callback fetch_all(state :: term) ::
              {:ok, data, new_state}
              | {:error, reason, new_state}
              | {:stop, reason, new_state}
            when data: List.t() | Stream.t(),
                 new_state: term,
                 reason: term

  @callback put_data(data :: any, state :: term) ::
              {:ok, new_state}
              | {:stop, reason, new_state}
            when new_state: term, reason: term

  @callback handle_info(msg :: term, state :: term) ::
              {:noreply, new_state}
              | {:stop, reason, new_state}
            when new_state: term, reason: term

  @moduledoc """

  The datastore is a source and a sink that will store data in memory or in
  a persitent manner, depending on the provided adapter.

  It can be made to listen to a primary source.

  """

  @spec fetch_all(atom | pid | {atom, any} | {:via, atom, any}) :: any
  def fetch_all(server) do
    GenServer.call(server, :fetch_all)
    |> IO.inspect(label: "fetched all")
  end

  def start_link(opts) do
    do_start(:start_link, opts)
  end

  def start(opts) do
    do_start(:start, opts)
  end

  @todo "use nimble :mod_arg type?"

  @opts_groups [
    gen: :gen_opts,
    impl: [
      module: [required: true, type: Opts.validator(:resolve_module)],
      init_arg: [default: []]
    ],
    self: [source: [type: Opts.validator(:source)]]
  ]

  defp do_start(kind, opts) do
    with {:ok, opts} <- Opts.group(opts, @opts_groups) do
      apply(GenServer, kind, [__MODULE__, opts, opts.gen])
    end
  end

  defmodule S do
    defstruct [:module, :substate, :subscription, :callbacks]
  end

  def init(opts) do
    opts |> IO.inspect(label: "opts")

    with {:ok, module, substate, callbacks} <- init_behaviour(opts.impl),
         {:ok, subscription} <- maybe_subscribe(Keyword.fetch(opts.self, :source)) do
      {:ok,
       %S{
         module: module,
         substate: substate,
         subscription: subscription,
         callbacks: callbacks
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @supported_callbacks [handle_info: 2]

  defp init_behaviour(opts) do
    module = Keyword.fetch!(opts, :module)
    init_arg = Keyword.fetch!(opts, :init_arg)

    if function_exported?(module, :init, 1) do
      case module.init(init_arg) do
        {:ok, state} ->
          callbacks =
            @supported_callbacks
            |> Enum.map(fn {cb, arity} -> {cb, function_exported?(module, cb, arity)} end)
            |> Map.new()

          {:ok, module, state, callbacks}

        {:error, _} = err ->
          err

        other ->
          {:error, {:bad_behaviour_return, other}}
      end
    else
      {:error, {:undef, module, :init, 1}}
    end
  end

  def maybe_subscribe({:ok, {source, opts}}) do
    Logger.debug("data store subcribing to source: #{inspect(source)}")

    Source.subscribe_monitor(source, opts)
  end

  def maybe_subscribe({:ok, nil}) do
    {:ok, nil}
  end

  def maybe_subscribe(:error) do
    {:ok, nil}
  end

  def handle_call(:fetch_all, _from, state) do
    %S{module: module, substate: substate} = state
    module.fetch_all(substate) |> check_reply(state)
  end

  def handle_info({:"$etl_listener", {_, ref}, {:data, data}}, state)
      when :erlang.map_get(:subscription, state) == ref do
    %S{module: module, substate: substate} = state
    module.put_data(data, substate) |> check_noreply(state)
  end

  def handle_info({:DOWN, ref, :process, _, reason}, state)
      when :erlang.map_get(:subscription, state) == ref do
    case reason do
      :normal -> {:stop, :normal, state}
      other -> {:stop, {:shutdown, {:source_exit, other}}, state}
    end
  end

  def handle_info(msg, %S{callbacks: %{handle_info: true}} = state) do
    %S{module: module, substate: substate} = state
    module.handle_info(msg, substate) |> check_noreply(state)
  end

  def handle_info(msg, state) do
    Logger.warn("unexpected info message: #{inspect(msg)}")
    {:noreply, state}
  end

  def terminate(reason, %S{module: module, substate: substate}) do
    if function_exported?(module, :terminate, 2) do
      module.terminate(reason, substate)
    else
      :ok
    end
  end

  defp check_reply({:ok, data, new_substate}, state) do
    {:reply, {:ok, data}, %S{state | substate: new_substate}}
  end

  defp check_reply({:error, reason, new_substate}, state) do
    {:reply, {:error, reason}, %S{state | substate: new_substate}}
  end

  defp check_reply({:stop, reason, new_substate}, state) do
    {:stop, reason, {:error, reason}, %S{state | substate: new_substate}}
  end

  defp check_noreply({:ok, new_substate}, state) do
    {:noreply, %S{state | substate: new_substate}}
  end

  defp check_noreply({:noreply, new_substate}, state) do
    {:noreply, %S{state | substate: new_substate}}
  end

  defp check_noreply({:stop, reason, new_substate}, state) do
    {:stop, reason, %S{state | substate: new_substate}}
  end
end
