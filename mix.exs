defmodule Ethyl.MixProject do
  use Mix.Project

  def project do
    [
      app: :ethyl,
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Ethyl.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gen_stage, "~> 1.0"}
      # {:absinthe, "~> 1.5"}
    ]
  end
end
