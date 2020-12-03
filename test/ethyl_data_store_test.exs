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
      assert true == Keyword.fetch!(opts, :replay_history)
      Logger.debug("primary source received: #{inspect(msg)}")
      {parent_pid, parent_ref} = parent
      send(parent_pid, {:subscribed, parent_ref, pid})
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
      Logger.debug("test store init init_arg: #{inspect(init_arg)}")
      send(parent, {:started, ref})
      {:ok, []}
    end

    def fetch_all(list) do
      {:ok, list, list}
    end

    def put_data(list, %{key: _} = record) do
      {:ok, [record | list]}
    end
  end

  test "data store is started, subscribes, receives data and is fetchable" do
    source_ref = make_ref
    store_ref = make_ref
    {:ok, source} = MicroFSM.start(MySource, {self(), source_ref}, debug: [:trace])

    {:ok, store} =
      DataStore.start(
        source: source,
        module: TestStore,
        init_arg: {self(), store_ref}
      )

    # test that the implementation module is called
    assert_receive {:started, ^store_ref}
    # test that the source is subscribed
    assert_receive {:subscribed, ^source_ref, ^store}
    # send data to source, wait a bit, and expect that data in the store. We
    # insert the key 1001 twice and expect it to be
    records = [
      %{key: 1000, name: "r0b07"},
      %{key: 1001, name: "PoorGuy"},
      %{key: 1001, name: "Replaced"}
    ]

    for rec <- records do
      send(source, {:forward, rec})
    end

    Process.sleep(100)

    assert {:ok, data} = DataStore.fetch_all(store)
    assert Enum.sort(data) === Enum.sort(records)
  end
end
