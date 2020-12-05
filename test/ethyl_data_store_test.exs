defmodule Ethyl.Source.DataStoreTest do
  use ExUnit.Case, async: true
  doctest Ethyl.Source.DataStore

  alias Ethyl.Source.DataStore
  alias Ethyl.MicroFSM
  alias Ethyl.Source.Emitter
  import Ethyl.TestHelper
  require Logger

  defmodule MySource do
    def init({parent_pid, parent_ref} = parent) do
      Logger.debug("primary source init")
      {:next, :await_sub, {parent, Emitter.new()}}
    end

    def await_sub({:"$etl_source", {pid, ref}, {:subscribe, opts}} = msg, {parent, em})
        when is_pid(pid) and is_reference(ref) do
      assert :some_val == Keyword.fetch!(opts, :some_opt)
      Logger.debug("primary source received: #{inspect(msg)}")
      {parent_pid, parent_ref} = parent
      send(parent_pid, {parent_ref, :subscribed, pid})
      {:ok, em} = Emitter.handle_subscribe(em, msg)
      {:next, :await_event, {parent, em}}
    end

    def await_event({:forward, event} = msg, {parent, em}) do
      Logger.debug("primary source received: #{inspect(msg)}")
      :ok = Emitter.emit(em, event)
      {:next, :await_event, {parent, em}}
    end

    def terminate(reason, _) do
      Logger.debug("primary source terminate: #{inspect(reason)}")
    end
  end

  defmodule TestStore do
    @behaviour Ethyl.Source.DataStore

    def init({parent, ref} = init_arg) do
      send_parent = &send(parent, {ref, &1})
      Logger.debug("test store init init_arg: #{inspect(init_arg)}")
      send_parent.(:started)
      {:ok, {send_parent, []}}
    end

    def fetch_all({_, list} = state) do
      {:ok, list, state}
    end

    def put_data(%{key: _} = record, {send_parent, list} = state) do
      {:ok, {send_parent, [record | list]}}
    end

    def handle_info(msg, {send_parent, _} = state) do
      send_parent.({:handled_info, msg})
      {:noreply, state}
    end

    def terminate(reason, {send_parent, _}) do
      send_parent.(:called_terminate)
    end
  end

  test "data store is started, subscribes, receives data and is fetchable" do
    source_ref = make_ref
    store_ref = make_ref
    {:ok, source} = MicroFSM.start(MySource, {self(), source_ref})

    {:ok, store} =
      DataStore.start(
        source: {source, some_opt: :some_val},
        module: TestStore,
        init_arg: {self(), store_ref}
      )

    store_monitor = Process.monitor(store)

    # test that the implementation module is called
    assert_receive {^store_ref, :started}
    # test that the source is subscribed
    assert_receive {^source_ref, :subscribed, ^store}
    # send data to source, wait a bit, and expect that data in the store. We
    # insert the key 1001 twice and expect it to be
    records = [
      %{key: 1000, name: "Alice"},
      %{key: 1001, name: "Bob"},
      %{key: 1002, name: "Carol"}
    ]

    for rec <- records do
      send(source, {:forward, rec})
    end

    Process.sleep(100)

    assert {:ok, data} = DataStore.fetch_all(store)
    assert Enum.sort(data) === Enum.sort(records)
    send(store, :some_unkown_info)
    Process.sleep(100)
    GenServer.stop(source, :byebye)
    assert_receive {^store_ref, {:handled_info, :some_unkown_info}}
    assert_receive {^store_ref, :called_terminate}
    assert_receive {:DOWN, ^store_monitor, :process, ^store, {:shutdown, {:source_exit, :byebye}}}
  end

  test "can start a data store without source" do
    assert {:ok, s1} =
             DataStore.start(
               module: TestStore,
               init_arg: {self(), make_ref()}
             )

    assert {:error,
            {:invalid_opts, [source: [type: {:custom, Ethyl.Opts, :validate_source, []}]],
             %NimbleOptions.ValidationError{
               key: :source,
               message: "invalid source: nil",
               value: nil
             }}} =
             DataStore.start(
               source: nil,
               module: TestStore,
               init_arg: {self(), make_ref()}
             )

    GenServer.stop(s1)
  end
end
