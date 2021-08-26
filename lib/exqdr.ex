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

  defp decode(response) do
    case Jason.decode(response.body) do
      {:ok, payload} ->
        payload

      {:error, why} = error ->
        error
    end
  end

  defp unpack(response) do
    case response.status_code do
      200 ->
        decode(response)

      _ ->
        {:error, response}

    end
  end

  def get(path, conn) do
    with {:ok, response} <- HTTPoison.get("#{conn}#{path}", @headers, []),
         # IO.inspect(response, label: "get_response_raw"),
         _status_code = 200 <- response.status_code,
         {:ok, payload} <- Jason.decode(response.body) do
      {:ok, payload}
    else
      error ->
        {:error, error}
    end
  end

  def post(path, data, conn) do
    
    with {:ok, request} <- Jason.encode(data) do
     case HTTPoison.post("#{conn}#{path}", request, @headers, []) do
      {:ok, response} ->
        case unpack(response) do
          %{"status" => "ok"} = payload -> 
            {:ok, payload}

          _ = badpayload ->
            badpayload
        end

      {:error, _} = error ->
        error
      end
    end
  end
end

defmodule Exqdr.Collection do
  @scroll_limit 2000

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
    payload = %{"delete_points" => %{"ids" => points}}
    post("/collections/" <> name, payload, conn)
  end

  def delete(point, name, conn) when is_integer(point) do
    delete([point], name, conn)
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
    case fetch!([id], name, conn) do
      [] -> nil
      list -> Enum.at(list, 0)
    end
  end

  def info(name, conn) do
    case get("/collections/#{name}", conn) do
      {:ok, %{"status" => "ok", "result" => res}} ->
        {:ok, res}

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

        scores_by_id = Enum.map(res["result"], &{&1["id"], &1["score"]}) |> Enum.into(%{})

        rows = fetch!(Map.keys(scores_by_id), name, conn) 
          |> Enum.map(fn x ->

            id = x["id"]
            score = scores_by_id[id]

            Map.put(x, "score", score)
          end)

        {:ok, rows}

      _ ->
        resp
    end
  end

  def recreate(params, conn) do
    drop(params, conn)
    create(params, conn)
  end

  def search(filter, offset, name, conn) do
    payload = %{"limit" => @scroll_limit, "offset" => offset, "filter" => filter}

    case post("/collections/#{name}/points/scroll", payload, conn) do
      {:ok, %{"result" => %{"next_page_offset" => new_ofs, "points" => points}}} ->

        case new_ofs do
          nil -> reform_results(points)
          _ -> reform_results(points) ++ search(filter, new_ofs, name, conn)
        end

      _ = e ->
        e
    end
  end

  def search(filter, name, conn) do
    search(filter, 0, name, conn)
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
    Enum.map(results, &reform_item/1)
  end
end

"""

future API:


defmodule LP.Repo.SearchSession
  defstruct id: 0, term: "", when: 0, user: 0

  def server(), do: "http://localhost:6033"
  def size(), do: 384
  def collection(), do: "search_session"
end

{:ok, listings} = Exqdr.Q.all(LP.Repo.SearchSession)
{:ok, listings} = Exqdr.Q.filter(LP.Repo.SearchSession, %{)

"""


