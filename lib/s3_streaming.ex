defmodule S3Streaming do
  require Logger

  def hash(bucket, key, parallel \\ 5) do
    stream(bucket, key, parallel)
    |> Enum.reduce(:crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1))
    |> :crypto.hash_final()
    |> Base.encode16()
    |> String.downcase()
  end

  def stream(bucket, key, parallel \\ 5) do
    Stream.resource(
      fn -> first(bucket, key, parallel) end,
      fn state -> next(state) end,
      fn _ -> :ok end
    )
  end

  defp first(bucket, key, parallel) do
    with parts <- head(bucket, key) |> Map.get("x-amz-mp-parts-count", "1") |> String.to_integer() do
      {bucket, key, parallel, parts, 1}
    end
  end

  defp next({bucket, key, parallel, parts, low}) when low <= parts do
    with high <- low + min(parallel - 1, parts - low) do
      Logger.info("Retrieving parts #{low} through #{high}")

      chunks =
        low..high
        |> Enum.map(fn partNumber ->
          Task.async(fn ->
            {partNumber, part(bucket, key, partNumber)}
          end)
        end)
        |> Task.await_many(60000)
        |> Enum.sort_by(fn {partNumber, _} -> partNumber end)
        |> Enum.map(fn {_, chunk} -> chunk.body end)

      {chunks, {bucket, key, parallel, parts, high + 1}}
    end
  end

  defp next(_), do: {:halt, nil}

  def head(bucket, key) do
    with op <- ExAws.S3.head_object(bucket, key),
         params <- op.params do
      Map.put(op, :params, Map.put(params, "partNumber", 1))
    end
    |> ExAws.request!()
    |> Map.get(:headers)
    |> Enum.into(%{})
  end

  def part(bucket, key, part) do
    with op <- ExAws.S3.get_object(bucket, key),
         params <- op.params do
      Map.put(op, :params, Map.put(params, "partNumber", part))
    end
    |> ExAws.request!()
  end
end
