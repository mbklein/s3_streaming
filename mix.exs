defmodule S3Streaming.MixProject do
  use Mix.Project

  def project do
    [
      app: :s3_streaming,
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:configparser_ex, "~> 4.0.0"},
      {:ex_aws, "~> 2.1"},
      {:ex_aws_s3, "~> 2.0"},
      {:httpoison, "~> 1.7"}
    ]
  end
end
