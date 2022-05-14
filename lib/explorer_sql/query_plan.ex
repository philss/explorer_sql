defmodule ExplorerSQL.QueryPlan do
  @moduledoc false
  # Holds all data to construct a SELECT query by the adapters.

  # Fields where based in the SELECT syntax of PostgreSQL:
  # https://www.postgresql.org/docs/current/sql-select.html
  defstruct columns: nil,
            from_item: nil,
            condition: nil,
            group_by: nil,
            having: nil,
            window: nil,
            order_by: nil,
            limit: nil,
            offset: nil
end
