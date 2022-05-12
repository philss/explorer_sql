defmodule ExplorerSQL.Adapters.Postgres do
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

  def to_sql(%ExplorerSQL.Backend.DataFrame{} = df) do
    # TODO: for security reasons, we need to scape the table name.
    "SELECT * FROM #{df.table}"
  end
end
