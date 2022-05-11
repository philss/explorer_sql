defmodule ExplorerSQL.Adapters.Postgres do
  def table_description(pid, table_name) do
    sql_statement = """
    SELECT c.column_name, c.data_type
    FROM information_schema.columns c
    WHERE c.table_name = $1
    ORDER BY c.ordinal_position
    """

    case Postgrex.query(pid, sql_statement, [table_name], []) do
      {:ok, %Postgrex.Result{rows: [_ | _] = rows}} ->
        columns_with_dtypes = for [column, type] <- rows, do: {column, translate_dtype(type)}

        {:ok, Enum.unzip(columns_with_dtypes)}

      {:ok, %Postgrex.Result{rows: []}} ->
        {:error, :table_not_found}

      {:error, error} ->
        {:error, {:db_error, error}}
    end
  end

  defp translate_dtype("integer"), do: :integer
  defp translate_dtype("float"), do: :float
  defp translate_dtype("decimal"), do: :float
  defp translate_dtype("text"), do: :string
  defp translate_dtype("varchar"), do: :string

  def to_sql(%ExplorerSQL.DataFrame{} = df) do
    "SELECT * FROM #{df.table}"
  end
end
