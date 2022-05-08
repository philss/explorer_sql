defmodule ExplorerSQLTest do
  use ExUnit.Case
  doctest ExplorerSQL

  test "greets the world" do
    assert ExplorerSQL.hello() == :world
  end
end
