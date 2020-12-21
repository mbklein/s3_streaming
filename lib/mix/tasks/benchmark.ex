defmodule Mix.Tasks.Benchmark do
  use Mix.Task

  def run(_) do
    Mix.Task.run("app.start")
    S3Streaming.benchmark()
  end
end
