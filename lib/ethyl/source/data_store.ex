defmodule Ethyl.Source.DataStore do
  use GenServer
  alias Ethyl.Utils
  require Ethyl.Utils
  alias Ethyl.Source

  @callback init(term) :: state when state: term
  @callback fetch_all(state :: term) ::
              {:ok, data, new_state}
              | {:error, reason, new_state}
              | {:stop, reason, new_state}
            when data: List.t() | Stream.t(),
                 new_state: term,
                 reason: term

  @callback put_data(state :: term, data :: any) :: {:ok, new_state} | {:stop, reason, new_state}
            when new_state: term, reason: term

  @moduledoc """

  The datastore is a source and a sink that will store data in memory or in
  a persitent manner, depending on the provided adapter.

  It can be made to listen to a primary source.

  """

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

  defp do_start(kind, opts) do
    {gen_opts, opts} = Utils.gen_opts(opts)
    {impl_spec, opts} = Utils.split_impl_spec(opts)
    apply(GenServer, kind, [__MODULE__, {impl_spec, opts}, gen_opts])
  end

  defmodule State do
    defstruct [:module, :substate, :subscription]
  end

  def init({impl_spec, opts}) do
    with {:ok, module, substate} <- Utils.init_behaviour(impl_spec),
         {:ok, subscription} <- maybe_subscribe(Keyword.fetch(opts, :source)) do
      {:ok, %State{module: module, substate: substate, subscription: subscription}}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  def maybe_subscribe({:ok, source}) when Utils.is_name(source) do
    pid = GenServer.whereis(source)
    ref = Process.monitor(pid)

    case Source.subscribe(pid, ref, replay_history: true) do
      :ok ->
        {:ok, ref}

      {:error, _} = err ->
        Process.demonitor(ref, [:flush])
        err
    end
  end

  def maybe_subscribe({:ok, nil}) do
    {:ok, nil}
  end

  def maybe_subscribe(:error) do
    {:ok, nil}
  end

  def handle_call(:fetch_all, _from, state) do
    %State{module: module, substate: substate} = state
    module.fetch_all(substate) |> check_reply(state)
  end

  def handle_info({:"$etl_listener", {_, ref}, {:data, data}}, state)
      when :erlang.map_get(:subscription, state) == ref do
    %State{module: module, substate: substate} = state
    module.put_data(substate, data) |> check_noreply(state)
  end

  defp check_reply({:ok, data, new_substate}, state) do
    {:reply, {:ok, data}, %State{state | substate: new_substate}}
  end

  defp check_reply({:error, reason, new_substate}, state) do
    {:reply, {:error, reason}, %State{state | substate: new_substate}}
  end

  defp check_reply({:stop, reason, new_substate}, state) do
    {:stop, reason, {:error, reason}, %State{state | substate: new_substate}}
  end

  defp check_noreply({:ok, new_substate}, state) do
    {:noreply, %State{state | substate: new_substate}}
  end

  defp check_noreply({:stop, reason, new_substate}, state) do
    {:stop, reason, %State{state | substate: new_substate}}
  end
end
