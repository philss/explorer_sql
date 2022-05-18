defmodule ExplorerSQL do
  @moduledoc """
  Documentation for `ExplorerSQL`.
  """

  alias ExplorerSQL.Adapters.Postgres, as: PG
  alias ExplorerSQL.Backend.DataFrame, as: SQLDF

  alias Explorer.DataFrame, as: DF

  @doc """
  Starts a new ExplorerSQL process.

  See `Postgrex.start_link/1` options.
  """
  def start_link(opts) do
    Postgrex.start_link(opts)
  end

  def table(pid, name) do
    with {:ok, {columns, dtypes}} <- PG.table_description(pid, name) do
      sql_df = %SQLDF{pid: pid, table: name, columns: columns, dtypes: dtypes}

      %Explorer.DataFrame{data: sql_df, groups: []}
    end
  end

  ## Introspection

  def names(%DF{data: %SQLDF{} = sql_df}), do: sql_df.columns

  def to_sql(%DF{data: %SQLDF{} = sql_df}), do: PG.to_sql(sql_df)

  ## Verbs

  def head(%DF{data: %SQLDF{}} = df, n_rows \\ 5) when is_integer(n_rows) do
    maybe_add_operation(df, {:head, [n_rows]})
  end

  def select(%DF{data: %SQLDF{}} = df, columns, keep_or_drop)
      when is_list(columns) and keep_or_drop in [:keep, :drop] do
    columns =
      if keep_or_drop == :keep do
        columns
      else
        names(df) -- columns
      end

    maybe_add_operation(df, {:select, [columns]})
  end

  defp maybe_add_operation(%DF{data: %SQLDF{} = sql_df} = df, operation) do
    case {operation, List.first(sql_df.operations)} do
      {operation, operation} ->
        df

      {operation, _last_is_other} ->
        %{df | data: %{sql_df | operations: [operation | sql_df.operations]}}
    end
  end
end
