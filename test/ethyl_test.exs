defmodule EthylTest do
  use ExUnit.Case, async: true
  doctest Ethyl

  test "greets the world" do
    assert Ethyl.hello() == :world
  end
end
