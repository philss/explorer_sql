defmodule ExplorerSQL.Adapters.Postgres do
  @moduledoc false

  @spec table_description(pid(), String.t()) ::
          {:ok, {list(String.t()), list(atom())}}
          | {:error, :table_not_found | {:db_error, term()}}
  def table_description(pid, table_name) do
    sql_statement = """
    SELECT c.column_name, c.data_type
    FROM information_schema.columns c
    WHERE c.table_name = $1
    ORDER BY c.ordinal_position
    """

    case Postgrex.query(pid, sql_statement, [table_name], []) do
      {:ok, %Postgrex.Result{rows: [_ | _] = rows}} ->
        columns_with_dtypes = for [column, type] <- rows, do: {column, type}

        {:ok, Enum.unzip(columns_with_dtypes)}

      {:ok, %Postgrex.Result{rows: []}} ->
        {:error, :table_not_found}

      {:error, error} ->
        {:error, {:db_error, error}}
    end
  end

  def to_sql(%ExplorerSQL.Backend.DataFrame{} = ldf) do
    ldf.operations
    |> Enum.reverse()
    |> Enum.reduce(basic_query_plan(ldf), fn {operation, _args}, plan ->
      case operation do
        :head ->
          %{plan | limit: "LIMIT 5"}
      end
    end)
    |> query_plan_to_sql()
  end

  defp basic_query_plan(ldf) do
    %ExplorerSQL.QueryPlan{columns: ldf.columns, from_item: ldf.table}
  end

  defp query_plan_to_sql(plan) do
    IO.iodata_to_binary([
      "SELECT * FROM ",
      quote_table(plan.from_item),
      if_do(plan.limit, [?\s, plan.limit])
    ])
  end

  # Helpers from EctoSQL's Postgres Adapter.
  # https://github.com/elixir-ecto/ecto_sql/blob/d53cf7e83020e9893c16437bb93d1e1ee9f68022/lib/ecto/adapters/postgres/connection.ex#L1259

  defp quote_table(name) do
    if String.contains?(name, "\"") do
      raise ArgumentError, "bad table name #{inspect(name)}"
    end

    [?", name, ?"]
  end

  defp if_do(condition, value) do
    if condition, do: value, else: []
  end
end
