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
      {:explorer, git: "https://github.com/elixir-nx/explorer.git"},
      # TODO: remove rustler after explorer 0.2 is released
      {:rustler, ">= 0.0.0"},
      {:postgrex, ">= 0.0.0"}
    ]
  end
end
