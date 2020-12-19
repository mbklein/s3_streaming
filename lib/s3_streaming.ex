defmodule S3Streaming do
  require Logger

  @threads 5

  defp first(bucket, key) do
    with parts <- head(bucket, key) |> Map.get("x-amz-mp-parts-count", "1") |> String.to_integer() do
      {bucket, key, parts, 1}
    end
  end

  defp next({bucket, key, parts, low}) when low <= parts do
    with high <- low + min(@threads - 1, parts - low) do
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
        |> Enum.map(fn {_, chunk} -> chunk end)

      {chunks, {bucket, key, parts, high + 1}}
    end
  end

  defp next(_), do: {:halt, nil}

  def stream(bucket, key) do
    Stream.resource(
      fn -> first(bucket, key) end,
      fn state -> next(state) end,
      fn _ -> :ok end
    )
  end

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
