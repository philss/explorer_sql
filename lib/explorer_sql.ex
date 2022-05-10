defmodule ExplorerSQL do
  @moduledoc """
  Documentation for `ExplorerSQL`.
  """

  @doc """
  Starts a new ExplorerSQL process.

  See `Postgrex.start_link/1` options.
  """
  def start_link(opts) do
    Postgrex.start_link(opts)
  end

  def table(pid, name) do
    # TODO: proper scape
    case Postgrex.query(pid, "SELECT * FROM #{name} LIMIT 1", [], []) do
      {:ok, %Postgrex.Result{rows: _}} ->
        {:ok, %ExplorerSQL.DataFrame{pid: pid, table: name}}

      {:error, _err} ->
        {:error, :table_not_found}
    end
  end
end
