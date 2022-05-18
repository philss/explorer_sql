defmodule ExplorerSQL.Query do
  @moduledoc false
  # Holds all data to construct a SELECT query by the adapters.

  # Fields where based in the SELECT syntax of PostgreSQL:
  # https://www.postgresql.org/docs/current/sql-select.html
  # If some operation is "replaced" by other, then the first one is "pushed" to a subquery
  defstruct columns: nil,
            from: nil,
            condition: nil,
            group_by: nil,
            having: nil,
            window: nil,
            order_by: nil,
            limit: nil,
            offset: nil

  def new(table) when is_binary(table) do
    %__MODULE__{from: table}
  end

  def new(lazy_frame) do
    %__MODULE__{from: lazy_frame.table}
  end

  def put_limit(%__MODULE__{} = plan, limit) when is_integer(limit) do
    %{plan | limit: to_string(limit)}
  end

  def put_columns(%__MODULE__{} = plan, columns) when is_list(columns) do
    if is_nil(plan.limit) do
      %{plan | columns: columns}
    else
      %__MODULE__{from: plan, columns: columns}
    end
  end
end
