defmodule ExplorerSQL.Backend.DataFrame do
  @moduledoc false
  defstruct table: nil, pid: nil, columns: [], dtypes: [], query: nil
end
