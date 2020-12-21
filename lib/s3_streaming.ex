defmodule S3Streaming do
  alias S3Streaming.{AWSStream, Meadow, Multipart}

  @files %{
    "738MB" =>
      {"meadow-s-ingest", "happiness-1607025377/inu-dil-29dad0e6-d7d6-4c4a-9465-94a21aea60a6.tif"},
    "113MB" =>
      {"meadow-s-ingest", "happiness-1607025377/inu-dil-5022f840-b035-4a6b-b484-49a4411924e9.tif"},
    "54MB" => {"meadow-s-ingest", "happiness-1607025377/BFMF_B34_F16_005_p001.tif"},
    "4MB" => {"meadow-s-ingest", "happiness-1607025377/Tillie.png"}
  }

  @chunk_sizes [128, 32, 16]
  @simultaneous_parts [20, 10, 5, 1]

  def benchmark do
    all_tests()
    |> Benchee.run(inputs: @files)
  end

  def all_tests do
    (aws_stream() ++ multipart() ++ meadow()) |> Enum.into(%{})
  end

  def aws_stream do
    Enum.map(@chunk_sizes, fn mb ->
      {"ExAws.stream!/2: #{mb}MB chunks",
       fn {bucket, key} -> AWSStream.hash(bucket, key, mb * 1024 * 1024) end}
    end)
  end

  def multipart do
    Enum.map(@simultaneous_parts, fn parts ->
      {"Multipart: #{parts} thread(s)",
       fn {bucket, key} -> Multipart.hash(bucket, key, parts) end}
    end)
  end

  def meadow do
    [{"Meadow.Utils.Stream", fn {bucket, key} -> Meadow.hash(bucket, key) end}]
  end
end
