defmodule ExplorerSQL do
  @moduledoc """
  Documentation for `ExplorerSQL`.
  """

  alias Explorer.DataFrame, as: DF

  alias ExplorerSQL.Adapters.Postgres, as: PG
  alias ExplorerSQL.Backend.DataFrame, as: SQLDF
  alias ExplorerSQL.Query

  @doc """
  Starts a new ExplorerSQL process.

  See `Postgrex.start_link/1` options.
  """
  def start_link(opts) do
    Postgrex.start_link(opts)
  end

  def table(pid, name) do
    with {:ok, {columns, dtypes}} <- PG.table_description(pid, name) do
      sql_df = %SQLDF{
        pid: pid,
        table: name,
        columns: columns,
        dtypes: dtypes,
        query: Query.new(name)
      }

      %Explorer.DataFrame{data: sql_df, groups: []}
    end
  end

  def collect(%DF{data: %SQLDF{} = sql_df}), do: PG.collect(sql_df)

  ## Introspection

  def names(%DF{data: %SQLDF{} = sql_df}), do: sql_df.columns

  def to_sql(%DF{data: %SQLDF{} = sql_df}), do: PG.to_sql(sql_df)

  ## Verbs

  def head(%DF{data: %SQLDF{}} = df, n_rows \\ 5) when is_integer(n_rows) do
    update_query(df, &Query.put_limit(&1, n_rows))
  end

  def select(%DF{data: %SQLDF{}} = df, columns, keep_or_drop)
      when is_list(columns) and keep_or_drop in [:keep, :drop] do
    columns =
      if keep_or_drop == :keep do
        columns
      else
        names(df) -- columns
      end

    update_query(df, &Query.put_columns(&1, columns))
  end

  defp update_query(df, new_plan_fun) when is_function(new_plan_fun) do
    new_plan = new_plan_fun.(df.data.query)

    %{df | data: %{df.data | query: new_plan}}
  end
end
