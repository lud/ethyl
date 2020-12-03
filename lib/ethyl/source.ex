defmodule Ethyl.Source do
  def subscribe(server, ref, opts \\ []) when is_list(opts) do
    msg = {:"$etl_source", {self(), ref}, {:subscribe, opts}}

    case send_to_name(server, msg) do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  defp send_to_name(server, msg) do
    case GenServer.whereis(server) do
      nil ->
        {:error, :noproc}

      pid ->
        send(pid, msg)
        :ok
    end
  end
end
