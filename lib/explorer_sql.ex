defmodule ExplorerSQL do
  @moduledoc """
  Documentation for `ExplorerSQL`.
  """

  alias ExplorerSQL.Adapters.Postgres, as: PG
  alias ExplorerSQL.DataFrame, as: SQLDF

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

  def to_sql(%DF{data: df}), do: PG.to_sql(df)
  def to_sql(%SQLDF{} = df), do: PG.to_sql(df)
end
