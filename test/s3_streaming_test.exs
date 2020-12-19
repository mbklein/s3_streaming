defmodule S3StreamingTest do
  use ExUnit.Case
  doctest S3Streaming

  test "greets the world" do
    assert S3Streaming.hello() == :world
  end
end
