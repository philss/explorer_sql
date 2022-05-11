defmodule ExplorerSQLTest do
  use ExUnit.Case, async: true
  doctest ExplorerSQL

  setup do
    opts = [
      database: "explorer_sql_test",
      username: System.get_env("PGUSER") || "postgres",
      password: System.get_env("PGUPASSWORD") || "postgres",
      backoff_type: :stop,
      prepare: :named,
      max_restarts: 0
    ]

    {:ok, pid} = ExplorerSQL.start_link(opts)

    {:ok, [pid: pid, options: opts]}
  end

  describe "table/2" do
    test "returns an explorer_sql dataframe if table exists", %{pid: pid} do
      assert {:ok, %ExplorerSQL.DataFrame{} = df} =
               ExplorerSQL.table(pid, "links")

      assert df.pid == pid
      assert df.table == "links"
      assert df.columns == ["id", "url", "clicks"]
      assert df.dtypes == [:integer, :string, :integer]
    end

    test "returns an error when table does not exist", %{pid: pid} do
      assert {:error, :table_not_found} = ExplorerSQL.table(pid, "posts")
    end
  end

  describe "to_sql/1" do
    test "returns a SQL statement selecting all data", %{pid: pid} do
      {:ok, df} = ExplorerSQL.table(pid, "links")
      statement = ExplorerSQL.to_sql(df)

      assert statement == "SELECT * FROM links"
    end
  end
end
