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

      assert statement ==
               String.trim("""
               SELECT * FROM "links"
               LIMIT 5
               """)
    end

    test "with `select` operation returns SQL statement selecting two columns", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.select(ldf, ["url", "clicks"], :keep)

      statement = ExplorerSQL.to_sql(ldf)

      assert statement ==
               String.trim("""
               SELECT "url", "clicks" FROM "links"
               """)
    end

    test "combine a `select` operation with a `head` operation", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.select(ldf, ["url", "clicks"], :keep)
      ldf = ExplorerSQL.head(ldf)

      statement = ExplorerSQL.to_sql(ldf)

      assert statement ==
               String.trim("""
               SELECT "url", "clicks" FROM "links"
               LIMIT 5
               """)
    end

    test "performs a subquery if `head` comes before `select`", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.head(ldf)
      ldf = ExplorerSQL.select(ldf, ["url", "clicks"], :keep)

      statement = ExplorerSQL.to_sql(ldf)

      assert statement ==
               String.trim("""
               SELECT "url", "clicks" FROM (
                 SELECT * FROM "links"
                 LIMIT 5)
               """)

      ldf = ExplorerSQL.head(ldf, 3)
      statement = ExplorerSQL.to_sql(ldf)

      assert statement ==
               String.trim("""
               SELECT "url", "clicks" FROM (
                 SELECT * FROM "links"
                 LIMIT 5)
               LIMIT 3
               """)
    end

    test "keep the columns from the last `select` if possible", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.select(ldf, ["url", "clicks"], :keep)
      ldf = ExplorerSQL.select(ldf, ["url"], :keep)

      statement = ExplorerSQL.to_sql(ldf)

      assert statement ==
               String.trim("""
               SELECT "url" FROM "links"
               """)

      ldf = ExplorerSQL.head(ldf, 12)

      statement = ExplorerSQL.to_sql(ldf)

      assert statement ==
               String.trim("""
               SELECT "url" FROM "links"
               LIMIT 12
               """)
    end
  end

  describe "head/1" do
    test "adds the head operation", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      assert ldf.data.operations == []

      ldf = ExplorerSQL.head(ldf)

      assert %Explorer.DataFrame{} = ldf

      assert ldf.data.operations == [{:head, [5]}]
    end

    test "adds the head operation with a custom number of rows", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      assert ldf.data.operations == []

      ldf = ExplorerSQL.head(ldf, 12)

      assert %Explorer.DataFrame{} = ldf

      assert ldf.data.operations == [{:head, [12]}]
    end

    test "does not add the head operation if it's already there", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.head(ldf)

      assert ldf.data.operations == [{:head, [5]}]

      ldf = ExplorerSQL.head(ldf)

      assert ldf.data.operations == [{:head, [5]}]
    end
  end

  describe "select/3" do
    test "adds a select operation with only two columns", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.select(ldf, ["url", "clicks"], :keep)

      assert ldf.data.operations == [{:select, [["url", "clicks"]]}]

      ldf = ExplorerSQL.select(ldf, ["url"], :keep)

      assert ldf.data.operations == [{:select, [["url"]]}, {:select, [["url", "clicks"]]}]
    end

    test "adds a select operation removing one column", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.select(ldf, ["id"], :drop)

      assert ldf.data.operations == [{:select, [["url", "clicks"]]}]

      ldf = ExplorerSQL.select(ldf, ["clicks"], :drop)

      assert ldf.data.operations == [{:select, [["id", "url"]]}, {:select, [["url", "clicks"]]}]
    end

    test "does not add the same select operation twice in a row", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.select(ldf, ["url", "clicks"], :keep)

      assert ldf.data.operations == [{:select, [["url", "clicks"]]}]

      ldf = ExplorerSQL.select(ldf, ["url", "clicks"], :keep)

      assert ldf.data.operations == [{:select, [["url", "clicks"]]}]
    end
  end
end
