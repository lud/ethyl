defmodule Ethyl.DelegateClauseTest do
  use ExUnit.Case

  defmodule BaseMod do
    def get_val(:a), do: {__MODULE__, "a"}
  end

  defmodule ChildMod do
    def get_val(:b), do: {__MODULE__, "b"}
    defdelegate get_val(x), to: BaseMod
  end

  test "base delegation works" do
    assert ChildMod.get_val(:b) === {ChildMod, "b"}
    assert ChildMod.get_val(:a) === {BaseMod, "a"}
    assert BaseMod.get_val(:a) === {BaseMod, "a"}
  end
end
