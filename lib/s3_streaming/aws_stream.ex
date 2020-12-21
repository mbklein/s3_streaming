defmodule S3Streaming.AWSStream do
  def hash(bucket, file, chunk_size \\ 134_217_728) do
    bucket
    |> ExAws.S3.download_file(
      file,
      :memory,
      chunk_size: chunk_size
    )
    |> ExAws.stream!()
    |> Stream.chunk_while(
      :crypto.hash_init(:sha256),
      &chunk_fn/2,
      &after_fn/1
    )
    |> Enum.take(1)
  end

  defp chunk_fn(chunk, state) do
    {:cont, :crypto.hash_update(state, chunk)}
  end

  defp after_fn(state) do
    {:cont,
     :crypto.hash_final(state)
     |> Base.encode16()
     |> String.downcase(), []}
  end
end
