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
    query_plan_to_sql(ldf.query, 0)
  end

  # TODO: consider using the `Inspect.Algebra` module for this.
  defp query_plan_to_sql(plan, level) do
    IO.iodata_to_binary([
      spaces(level),
      "SELECT ",
      if(plan.columns, do: quote_columns(plan.columns), else: ?*),
      " FROM ",
      if(match?(%ExplorerSQL.Query{}, plan.from),
        do: subquery_to_sql(plan.from, level + 1),
        else: quote_table(plan.from)
      ),
      if_do(plan.limit, [?\n, spaces(level), "LIMIT ", plan.limit])
    ])
  end

  defp subquery_to_sql(subquery, level) do
    [?(, ?\n, query_plan_to_sql(subquery, level), ?)]
  end

  defp spaces(level) do
    Stream.cycle([?\s]) |> Enum.take(level * 2)
  end

  defp quote_columns(columns) do
    Enum.map_join(columns, ", ", &quote_name/1)
  end

  # Helpers from EctoSQL's Postgres Adapter.
  # https://github.com/elixir-ecto/ecto_sql/blob/d53cf7e83020e9893c16437bb93d1e1ee9f68022/lib/ecto/adapters/postgres/connection.ex#L1259

  defp quote_table(name) do
    if String.contains?(name, "\"") do
      raise ArgumentError, "bad table name #{inspect(name)}"
    end

    [?", name, ?"]
  end

  defp quote_name(name) do
    if String.contains?(name, "\"") do
      raise ArgumentError, "bad field name #{inspect(name)}"
    end

    [?", name, ?"]
  end

  defp if_do(condition, value) do
    if condition, do: value, else: []
  end
end
