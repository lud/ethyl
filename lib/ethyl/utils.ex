defmodule Ethyl.Utils do
  def fetch_existing_atom(str) when is_binary(str) do
    {:ok, String.to_existing_atom(str)}
  rescue
    _ -> :error
  end

  def fetch_existing_atom!(str) when is_binary(str) do
    case fetch_existing_atom(str) do
      {:ok, atom} -> atom
      :error -> raise ArgumentError, message: ~s(could not cast "#{str}" as existing atom)
    end
  end

  def cast_atom!(string) when is_binary(string) do
    fetch_existing_atom!(string)
  end

  def cast_atom!(atom) when is_atom(atom) do
    atom
  end

  defguard is_name(name)
           when is_pid(name) or
                  (is_atom(name) and not (name in [nil, true, false, :undefined])) or
                  (elem(name, 0) == :via and tuple_size(name) == 3) or
                  (elem(name, 0) == :global and tuple_size(name) == 2)
end

defmodule Ethyl.Utils.StructDelegate do
  defmacro __using__(_) do
    quote do
      defdelegate fetch(term, key), to: Map
      defdelegate get(term, key, default), to: Map
      defdelegate get_and_update(term, key, fun), to: Map
    end
  end
end
