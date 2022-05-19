defmodule ExplorerSQLTest do
  use ExUnit.Case, async: true
  doctest ExplorerSQL

  alias Explorer.DataFrame, as: DF

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
                 LIMIT 5) AS subquery_1
               """)

      ldf = ExplorerSQL.head(ldf, 3)
      statement = ExplorerSQL.to_sql(ldf)

      assert statement ==
               String.trim("""
               SELECT "url", "clicks" FROM (
                 SELECT * FROM "links"
                 LIMIT 5) AS subquery_1
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

  describe "collect/1" do
    test "without a query returns a new DF with all data from table", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      {:ok, df} = ExplorerSQL.collect(ldf)

      assert DF.names(df) == ["id", "url", "clicks"]

      assert %{
               "id" => [1, 2, 3],
               "clicks" => [42000, 51345, 63107],
               "url" => [
                 "https://elixir-lang.org",
                 "https://github.com/elixir-nx",
                 "https://github.com/elixir-nx/explorer"
               ]
             } = Explorer.DataFrame.to_columns(df)
    end

    test "select returns a new DF with some columns from table", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.select(ldf, ["url", "clicks"], :keep)
      {:ok, df} = ExplorerSQL.collect(ldf)

      assert DF.names(df) == ["url", "clicks"]

      assert %{
               "clicks" => [42000, 51345, 63107],
               "url" => [
                 "https://elixir-lang.org",
                 "https://github.com/elixir-nx",
                 "https://github.com/elixir-nx/explorer"
               ]
             } = Explorer.DataFrame.to_columns(df)
    end

    test "head limits the results of the query", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.head(ldf, 1)

      {:ok, df} = ExplorerSQL.collect(ldf)

      assert %{
               "clicks" => [42000],
               "id" => [1],
               "url" => [
                 "https://elixir-lang.org"
               ]
             } = Explorer.DataFrame.to_columns(df)
    end

    test "head before a select also limits the results of the query", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.head(ldf, 1)
      # Changing order of columns also affects the final DF
      ldf = ExplorerSQL.select(ldf, ["clicks", "url"], :keep)

      {:ok, df} = ExplorerSQL.collect(ldf)

      assert DF.names(df) == ["clicks", "url"]

      assert %{
               "clicks" => [42000],
               "url" => [
                 "https://elixir-lang.org"
               ]
             } = Explorer.DataFrame.to_columns(df)
    end
  end

  describe "head/1" do
    test "adds the head operation", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.head(ldf)

      assert %Explorer.DataFrame{} = ldf

      assert ldf.data.query.limit == "5"
    end

    test "adds the head operation with a custom number of rows", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.head(ldf, 12)

      assert %Explorer.DataFrame{} = ldf

      assert ldf.data.query.limit == "12"
    end
  end

  describe "select/3" do
    test "adds a select to query with given columns", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.select(ldf, ["url", "clicks"], :keep)

      assert ldf.data.query.columns == ["url", "clicks"]

      ldf = ExplorerSQL.select(ldf, ["url"], :keep)

      assert ldf.data.query.columns == ["url"]
    end

    test "adds a select operation removing one column", %{pid: pid} do
      ldf = ExplorerSQL.table(pid, "links")
      ldf = ExplorerSQL.select(ldf, ["id"], :drop)

      assert ldf.data.query.columns == ["url", "clicks"]

      ldf = ExplorerSQL.select(ldf, ["clicks"], :drop)

      assert ldf.data.query.columns == ["id", "url"]
    end
  end
end
