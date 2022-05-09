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
end
