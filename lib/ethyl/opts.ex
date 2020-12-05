defmodule Ethyl.Opts do
  @type group_schema :: NimbleOptions.schema() | :gen_opts
  @type schema_tuple :: {atom, group_schema()}
  @type schemas :: [schema_tuple()]

  require Ethyl.Utils, as: Utils

  @spec group(Keyword.t(), schemas) :: {:ok, map} | {:error, reason :: term}
  def group(opts, schemas) when is_list(opts) do
    Enum.reduce(schemas, {:ok, %{}, opts}, fn
      {key, schema}, {:ok, acc, opts} ->
        case take_group(opts, schema) do
          {:ok, {data, rest}} -> {:ok, Map.put(acc, key, data), rest}
          {:error, reason} -> {:error, {:invalid_opts, schema, reason}}
        end

      _, {:error, _} = err ->
        err
    end)
    |> case do
      {:ok, acc, rest} -> {:ok, Map.update(acc, :_rest, rest, &Keyword.merge(rest, &1))}
      {:error, _} = err -> err
    end
  end

  defp take_group(opts, :gen_opts) do
    {:ok, split_gen_opts(opts)}
  end

  defp take_group(opts, schema) when is_list(schema) do
    {opts, rest} = Keyword.split(opts, Keyword.keys(schema))

    case NimbleOptions.validate(opts, schema) do
      {:ok, opts} -> {:ok, {opts, rest}}
      {:error, %NimbleOptions.ValidationError{}} = err -> err
    end
  end

  @spec gen_opts(Keyword.t()) :: GenServer.options()
  def gen_opts(opts) when is_list(opts) do
    Keyword.take(opts, [:debug, :name, :timeout, :spawn_opt, :hibernate_after])
  end

  @spec split_gen_opts(Keyword.t()) :: {GenServer.options(), rest :: Keyword.t()}
  def split_gen_opts(opts) when is_list(opts) do
    Keyword.split(opts, [:debug, :name, :timeout, :spawn_opt, :hibernate_after])
  end

  def resolve_module("Elixir." <> _ = as_string) do
    case Ethyl.Utils.fetch_existing_atom(as_string) do
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

  def resolve_name(name) when is_pid(name) do
    {:ok, name}
  end

  def resolve_name(name) when Utils.is_name(name) do
    case GenServer.whereis(name) do
      nil -> {:error, :noproc}
      pid when is_pid(pid) -> {:ok, pid}
    end
  end

  def validator(:resolve_module) do
    {:custom, __MODULE__, :validate_module, []}
  end

  def validator(:source) do
    {:custom, __MODULE__, :validate_source, []}
  end

  def validate_module(val) do
    case resolve_module(val) do
      {:ok, module} ->
        {:ok, module}

      {:error, reason} ->
        {:error, "could not resolve module #{inspect(val)}: #{inspect(reason)}"}
    end
  end

  def validate_source(name) when Utils.is_name(name) do
    {:ok, {name, []}}
  end

  def validate_source({name, opts} = source) when Utils.is_name(name) and is_list(opts) do
    {:ok, source}
  end

  def validate_source(invalid) do
    {:error, "invalid source: #{inspect(invalid)}"}
  end
end
