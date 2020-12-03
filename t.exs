defmodule SocketListUpdater do
  def start_link(producer \\ nil) do
    spawn_link(fn -> init(producer) end)
  end

  defp init(producer) do
    loop(producer)
  end

  defp loop(producer) do
    IO.puts("sending list")
    send(producer, {:new_list, generate_list()})
    # Send a list every minute, here every second
    Process.sleep(999)
    loop(producer)
  end

  defp generate_list() do
    # Stream.iterate(0, &(&1 + 1))
    # # The length of the list
    0..10
    |> Enum.map(fn code -> %{code: code} end)
  end
end

defmodule Counter do
  use GenStage

  def start_link(number \\ 0) do
    GenStage.start_link(__MODULE__, number)
  end

  def init(counter) do
    send(self(), :check)
    send(self(), :init_big)
    {:producer, counter}
  end

  def handle_demand(demand, counter) when demand > 0 do
    # If the counter is 3 and we ask for 2 items, we will
    # emit the items 3 and 4, and set the state to 5.
    # demand = max(1, Enum.random(round(raw_demand * 0.1)..round(raw_demand * 1.1)))
    # IO.puts("demand #{raw_demand} => #{demand}")
    events = Enum.to_list(counter..(counter + demand - 1))
    {:noreply, events, counter + demand}
  end

  def handle_info(:check, counter) do
    Process.send_after(self, :check, 1000)
    {:noreply, [counter], counter + 1}
  end

  def handle_info(:init_big, counter) do
    {:noreply, Enum.to_list(counter..(counter + 99)), counter + 100}
  end
end

defmodule Cumul do
  @interval 200
  def start_link do
    spawn_link(fn ->
      send(self(), :print)
      Process.register(self(), __MODULE__)
      loop({%{}, 0})
    end)
  end

  defp loop({map, total} = state) do
    receive do
      :print ->
        if total > 0 do
          print(state)
        end

        Process.send_after(self, :print, @interval)
        loop(state)

      {:add, name} = msg ->
        state = {
          Map.update(map, name, 1, &(&1 + 1)),
          total + 1
        }

        loop(state)
    end
  end

  defp print({map, total}) do
    ("rates: " <>
       (map
        |> Enum.map(fn {k, v} -> "#{k}=#{round(v / total * 100)}" end)
        |> Enum.join(", ")))
    |> IO.puts()

    ("sums: " <>
       (map
        |> Enum.map(fn {k, v} -> "#{k}=#{v}" end)
        |> Enum.join(", ")))
    |> IO.puts()
  end
end

defmodule GenstageExample.Producer do
  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_arg) do
    {:producer, :some_state}
  end

  def handle_info({:new_list, list}, state) do
    {:noreply, list, state}
  end

  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end
end

defmodule GenstageExample.Consumer do
  use ConsumerSupervisor

  # Starts consumer with his code
  def start_link(producer, name, sleep \\ 500) do
    ConsumerSupervisor.start_link(__MODULE__, {producer, name, sleep})
  end

  def init({producer, name, sleep}) do
    # Note: By default the restart for a child is set to :permanent
    # which is not supported in ConsumerSupervisor. You need to explicitly
    # set the :restart option either to :temporary or :transient.
    children = [
      %{
        id: GenstageExample.Printer,
        start: {GenstageExample.Printer, :start_link, [name, sleep]},
        restart: :transient
      }
    ]

    opts = [strategy: :one_for_one, subscribe_to: [{producer, max_demand: 100}]]
    ConsumerSupervisor.init(children, opts)
  end
end

defmodule GenstageExample.Printer do
  def start_link(name, sleep, event) do
    Task.start_link(fn -> print(name, sleep, event) end)
  end

  defp print(name, sleep, code) do
    Process.sleep(sleep)
    send(Cumul, {:add, name})
    # code |> IO.inspect(label: name)
  end
end

Cumul.start_link()
{:ok, counter} = Counter.start_link()
GenstageExample.Consumer.start_link(counter, "Joe")
GenstageExample.Consumer.start_link(counter, "Robert")
GenstageExample.Consumer.start_link(counter, "Mike")
GenstageExample.Consumer.start_link(counter, "A1", 1000)
GenstageExample.Consumer.start_link(counter, "A2", 1000)
# SocketListUpdater.start_link(GenstageExample.Producer)

Process.sleep(:infinity)
