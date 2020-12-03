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
    String.to_existing_atom(string)
  end

  def cast_atom!(atom) when is_atom(atom) do
    atom
  end

  def resolve_module("Elixir." <> rest = as_string) do
    case fetch_existing_atom(as_string) do
      {:ok, atom} -> resolve_module(atom)
      :error -> {:error, :nomodule}
    end
  end

  def resolve_module(str) when is_binary(str) do
    resolve_module("Elixir." <> str)
  end

  def resolve_module(module) when is_atom(module) do
    case Code.ensure_loaded(module) do
      {:module, module} -> {:ok, module}
      {:error, _} = err -> err
    end
  end

  def resolve_module!(spec) do
    case resolve_module(spec) do
      {:ok, module} ->
        module

      {:error, reason} ->
        raise ArgumentError,
          message: ~s(could not resolve module from "#{inspect(spec)}": #{inspect(reason)})
    end
  end

  def init_behaviour({module, arg}) when is_atom(module) do
    if function_exported?(module, :init, 1) do
      case module.init(arg) do
        {:ok, state} -> {:ok, module, state}
        {:error, _} = err -> err
        other -> {:error, {:bad_behaviour_return, other}}
      end
    else
      {:error, {:undef, module, :init, 1}}
    end
  end

  @spec gen_opts(Keyword.t()) :: {GenServer.options(), Keyword.t()}
  def gen_opts(opts) when is_list(opts) do
    Keyword.split(opts, [:debug, :name, :timeout, :spawn_opt, :hibernate_after])
  end

  @doc """
  Expects the `:module` key, and optionally the `:init_arg` key from a
  `Keyword`.
  Returns a tuple with
  - the implementation module and an initialization argument (default to `[]`)
    in a sub-tuple
  - the rest of the options without the aforementioned keys
  """
  @spec split_impl_spec(Keyword.t()) :: {{module, init_arg :: any}, Keyword.t()}
  def split_impl_spec(opts) when is_list(opts) do
    {took, rest} = Keyword.split(opts, [:module, :init_arg])

    module = Keyword.fetch!(took, :module)
    module = resolve_module!(module)
    init_arg = Keyword.get(took, :init_arg, [])
    {{module, init_arg}, rest}
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
