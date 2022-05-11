defmodule ExplorerSQL.MixProject do
  use Mix.Project

  def project do
    [
      app: :explorer_sql,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:explorer, ">= 0.0.0"},
      {:postgrex, ">= 0.0.0"}
    ]
  end
end
