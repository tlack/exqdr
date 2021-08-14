defmodule Exqdr do
  @moduledoc """
  Simple wrapper for [Qdrant's](https://qdrant.tech/) HTTP API.

  Qdrant is a vector database for search applications.
  """

  def version!(qdr_url) do
    {:ok, %{"version" => ver}} = Exqdr.Lowlevel.get("", qdr_url)
    ver
  end

  def collections(conn) do
    case Exqdr.Lowlevel.get("/collections", conn) do
      {:ok, %{"status" => "ok", "result" => %{"collections" => list}}} ->
        {:ok, Enum.map(list, fn x -> x["name"] end)}

      _ = e ->
        e
    end
  end
end

defmodule Exqdr.Lowlevel do
  @headers %{"Content-Type": "application/json"}
  def get(path, conn) do
    with {:ok, response} <- HTTPoison.get("#{conn}#{path}", @headers, []),
         #IO.inspect(response, label: "get_response_raw"),
         status_code = 200 <- response.status_code,
         {:ok, payload} <- Jason.decode(response.body) do
      {:ok, payload}
      |> IO.inspect(label: "get: #{path}")
    else
      error ->
        {:error, error}
    end
  end

  def post(path, data, conn) do
    with {:ok, request} <- Jason.encode(data),
         #IO.inspect(request, label: "encoded_request"),
         {:ok, response} <- HTTPoison.post("#{conn}#{path}", request, @headers, []),
         #IO.inspect(response, label: "post_response_raw"),
         200 = response.status_code,
         {:ok, payload} <- Jason.decode(response.body),
         %{"status" => "ok"} = payload do
      {:ok, payload}
      # |> IO.inspect(label: "post: #{path} / #{Jason.encode!(data)}")
    else
      error ->
        {:error, error}
    end
  end

  def flatten_result(%{"id" => id, "payload" => payload, "vector" => vector} = result) do
    Map.merge(%{"id" => id, "vector" => vector}, payload)
  end
end

defmodule Exqdr.Collection do
  import Exqdr.Lowlevel

  def create(params, conn) do
    post("/collections", %{"create_collection" => params}, conn)
  end

  def drop(coll_name, conn) when is_binary(coll_name) do
    post("/collections", %{"delete_collection" => coll_name}, conn)
  end

  def drop(params, conn) when is_map(params) do
    post("/collections", %{"delete_collection" => params["name"]}, conn)
  end

  def all(name, conn) do
    search(%{}, name, conn)
  end

  def delete(points, name, conn) when is_list(points) do
    payload = %{"delete_points" => %{"points" => points}}
    post("/collections/" <> name, payload, conn)
  end

  def delete(point, name, conn) when is_integer(point) do
    upsert([point], name, conn)
  end


  def fetch!(ids, name, conn) when is_list(ids) do
    res =
      post(
        "/collections/#{name}/points?with_vector=true&with_payload=true",
        %{"ids" => ids},
        conn
      )

    case res do
      {:ok, %{"status" => "ok", "result" => r}} ->
        reform_results(r)

      _other ->
        nil
    end
  end

  def fetch!(id, name, conn) do
    fetch!([id], name, conn) |> Map.values() |> Enum.at(0)
  end

  def info(id, name, conn) do
    case get("/collections/#{name}", conn) do
      {:ok, %{"status" => "ok", "result" => res}} ->
        res
      _ = e ->
        e
    end
  end

  def rank(params, name, conn) do
    post("/collections/#{name}/points/search", params, conn)
  end

  def rank_and_fetch(params, name, conn) do
    # IO.inspect(params, label: "rank_and_fetch_query")
    resp = post("/collections/#{name}/points/search", params, conn)
    # |> IO.inspect(label: "rank_and_fetch_first_result")
    case resp do
      {:ok, res} ->
        matches = Enum.map(res["result"], &{&1["id"], &1["score"]})

        {ids, _scores} = Enum.unzip(matches)

        rows = fetch!(ids, name, conn)

        # IO.inspect(rows, label: "got_fetch_response")

        new_rows =
          Enum.map(
            matches,
            fn {id, score} ->
              Map.get(rows, id)
              |> Map.put("score", score)
            end
          )

        {:ok, %{"status" => "ok", "result" => new_rows}}

      _ ->
        resp
    end
  end

  def recreate(params, conn) do
    drop(params, conn)
    create(params, conn)
  end

  def search(filter, name, conn) do
    payload = %{"limit" => 1000, "offset" => 0, "filter" => %{}}
    case post("/collections/#{name}/points/scroll", payload, conn) do

      {:ok, %{"result" => %{"points" => points}}} ->
        reform_results(points)

      _ = e ->
        e

    end
  end

  def status(name, conn) do
    get("/collections/" <> name, conn)
  end

  def upsert(points, name, conn) when is_list(points) do
    payload = %{"upsert_points" => %{"points" => points}}
    post("/collections/" <> name, payload, conn)
  end

  def upsert(points, name, conn) do
    upsert([points], name, conn)
  end

  def upsert_wait(points, name, conn) when is_list(points) do
    payload = %{"upsert_points" => %{"points" => points}}
    post("/collections/" <> name <> "?wait=true", payload, conn)
  end

  def upsert_wait(points, name, conn) do
    upsert_wait([points], name, conn)
  end

  defp reform_item(item) do
    keys = Map.keys(item["payload"])

    (Enum.map(keys, fn k ->
       {k, item["payload"][k]["value"] |> Enum.at(0)}
     end) ++
       [{"vector", item["vector"]}, {"id", item["id"]}])
    |> Enum.into(%{})
  end

  defp reform_results(results) do
    Enum.map(results, fn r ->
      {r["id"], reform_item(r)}
    end)
    |> Enum.into(%{})
  end
end
