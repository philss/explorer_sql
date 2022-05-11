defmodule ExplorerSQL do
  @moduledoc """
  Documentation for `ExplorerSQL`.
  """

  alias ExplorerSQL.Adapters.Postgres, as: PG
  alias ExplorerSQL.DataFrame, as: DF

  @doc """
  Starts a new ExplorerSQL process.

  See `Postgrex.start_link/1` options.
  """
  def start_link(opts) do
    Postgrex.start_link(opts)
  end

  def table(pid, name), do: PG.table_description(pid, name)

  def to_sql(%DF{} = df), do: PG.to_sql(df)
end
