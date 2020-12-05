defmodule Ethyl.OptsTest do
  use ExUnit.Case, async: true
  doctest Ethyl.Opts
  alias Ethyl.Opts

  test "extract gen_opts, opts are consumed" do
    assert {:ok, %{a: [{:name, :joe}], b: [], _rest: []}} =
             Opts.group([name: :joe], a: :gen_opts, b: :gen_opts)

    assert {:ok, %{a: [], b: []}} = Opts.group([], a: :gen_opts, b: :gen_opts)
  end

  test "validate with nimble" do
    schema = [
      name: [type: :string, required: true],
      age: [type: :integer, default: 99]
    ]

    assert {:ok, %{my_opts: opts}} = Opts.group([name: "lud"], my_opts: schema)
    assert "lud" == opts[:name]
    assert 99 == opts[:age]

    assert {:error,
            {:invalid_opts, ^schema,
             %NimbleOptions.ValidationError{
               key: :name,
               keys_path: [],
               message: "required option :name not found, received options: []",
               value: nil
             }}} = Opts.group([], my_opts: schema, other: :gen_opts)
  end
end
