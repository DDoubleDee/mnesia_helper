defmodule MnesiaHelper.MixProject do
  use Mix.Project

  def project do
    [
      app: :mnesia_helper,
      version: "1.1.3",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      name: "MnesiaHelper",
      source_url: "https://github.com/DDoubleDee/mnesia_helper/",
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :mnesia]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.14", only: :dev, runtime: false}
    ]
  end

  defp description() do
    "A short and easy to use library to help out with erlang's :mnesia."
  end

  defp package() do
    [
      name: "mnesia_helper",
      # These are the default files included in the package
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE),
      licenses: ["GPL-3.0-or-later"],
      links: %{"GitHub" => "https://github.com/DDoubleDee/mnesia_helper/"}
    ]
  end
end
