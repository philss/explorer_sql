defmodule ExplorerSQL.QueryPlan do
  @moduledoc false
  # Holds all data to construct a SELECT query by the adapters.

  # Fields where based in the SELECT syntax of PostgreSQL:
  # https://www.postgresql.org/docs/current/sql-select.html
  # If some operation is "replaced" by other, then the first one is "pushed" to a subquery
  defstruct columns: nil,
            from: nil,
            subquery: nil,
            condition: nil,
            group_by: nil,
            having: nil,
            window: nil,
            order_by: nil,
            limit: nil,
            offset: nil

  def new(lazy_frame) do
    %__MODULE__{from: lazy_frame.table}
  end

  def put_limit(%__MODULE__{} = plan, limit_string) do
    %{plan | limit: limit_string}
  end

  def put_columns(%__MODULE__{} = plan, columns_iodata) do
    if is_nil(plan.limit) do
      %{plan | columns: columns_iodata}
    else
      %__MODULE__{subquery: plan, columns: columns_iodata}
    end
  end
end
