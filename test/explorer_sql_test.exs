defmodule ExplorerSQLTest do
  use ExUnit.Case, async: true
  doctest ExplorerSQL

  setup do
    # TODO: create a helper for pg connection
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
    test "returns a dataframe if table exists", %{pid: pid} do
      assert %Explorer.DataFrame{data: %ExplorerSQL.Backend.DataFrame{} = sql_df} =
               ExplorerSQL.table(pid, "links")

      assert sql_df.pid == pid
      assert sql_df.table == "links"
      assert sql_df.columns == ["id", "url", "clicks"]
      assert sql_df.dtypes == ["integer", "text", "integer"]
    end

    test "returns an error when table does not exist", %{pid: pid} do
      assert {:error, :table_not_found} = ExplorerSQL.table(pid, "posts")
    end
  end

  describe "to_sql/1" do
    test "returns a SQL statement selecting all data", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      statement = ExplorerSQL.to_sql(ldf)

      assert statement == "SELECT * FROM \"links\""
    end

    test "with `head` operation returns SQL statement selecting the first five lines", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.head(ldf)

      statement = ExplorerSQL.to_sql(ldf)

      assert statement == "SELECT * FROM \"links\" LIMIT 5"
    end
  end

  describe "head/1" do
    test "adds the head operation", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      assert ldf.data.operations == []

      ldf = ExplorerSQL.head(ldf)

      assert %Explorer.DataFrame{} = ldf

      assert ldf.data.operations == [{:head, []}]
    end

    test "does not add the head operation if it's already there", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.head(ldf)

      assert ldf.data.operations == [{:head, []}]

      ldf = ExplorerSQL.head(ldf)

      assert ldf.data.operations == [{:head, []}]
    end
  end
end
