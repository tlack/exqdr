defmodule ExqdrTest do
  use ExUnit.Case
  doctest Exqdr

  @test_server "http://localhost:6333"
  @test_col_1 "test"
  @test_col_2 "testnovec"
  @num_test_rows 100_000
  @num_queries 50_000

  test "1 qdr responds" do
    {:ok, %{"version" => ver}} = resp = Exqdr.Lowlevel.get("", @test_server)

    IO.inspect(resp, label: :index_response)

    ^ver = Exqdr.version!(@test_server)
  end

  test "2 recreate collection" do
    {:ok, _} =
      Exqdr.Collection.recreate(
        %{"name" => @test_col_1, "vector_size" => 4, "distance" => "Dot"},
        @test_server
      )
      |> IO.inspect(label: :create_response)

    {:ok, _} =
      Exqdr.Collection.status(@test_col_1, @test_server)
      |> IO.inspect(label: :status_response)
  end

  test "3 upsert" do
    # long_string = String.duplicate("Oh my god. This is definitely going to break the database", 2000)
    # IO.inspect(byte_size(long_string), label: "test string length")
    long_string = "NE"

    points = [
      %{
        "id" => 1,
        "vector" => [-1, -1, -1, -1],
        "payload" => %{"dir" => %{"type" => "keyword", "value" => "SW"}}
      },
      %{
        "id" => 2,
        "vector" => [1, 1, 1, 1],
        "payload" => %{"dir" => %{"type" => "keyword", "value" => long_string}}
      }
    ]

    {:ok, _} =
      Exqdr.Collection.upsert(points, @test_col_1, @test_server)
      |> IO.inspect(label: :upsert_response)

    %{"id" => 1} =
      Exqdr.Collection.fetch!(1, @test_col_1, @test_server)
      |> IO.inspect(label: :fetch_one)

    search = %{"vector" => [-0.5, -0.5, -0.5, -0.5], "top" => 3}

    {:ok, _} =
      resp1 =
      Exqdr.Collection.rank(search, @test_col_1, @test_server)
      |> IO.inspect(label: :search1_response)

    {:ok, %{"result" => [%{"id" => 1, "score" => 2.0}, %{"id" => 2, "score" => -2.0}]}} == resp1

    search2 = %{"vector" => [0.5, 0.5, 0.5, 0.5], "top" => 3}

    {:ok, _} =
      resp2 =
      Exqdr.Collection.rank(search2, @test_col_1, @test_server)
      |> IO.inspect(label: :search2_response)

    {:ok, %{"result" => [%{"id" => 2, "score" => 2.0}, %{"id" => 1, "score" => -2.0}]}} == resp2

    search3 = %{"vector" => [0.5, 0.5, 0.5, 0.5], "top" => 3}

    {:ok, payload} =
      resp2 =
      Exqdr.Collection.rank_and_fetch(search3, @test_col_1, @test_server)
      |> IO.inspect(label: :search3_response)

    result = Map.get(payload, "result")
  end

  test "4 collections without vectors" do
    {:ok, _} =
      Exqdr.Collection.recreate(
        %{"name" => @test_col_2, "vector_size" => 0, "distance" => "Dot"},
        @test_server
      )
      |> IO.inspect(label: :create_novec_response)

    points2 = [
      %{"id" => 1, "vector" => [], "payload" => %{"name" => "Tom", "email" => "test"}}
    ]

    {:ok, _} =
      Exqdr.Collection.upsert(points2, @test_col_2, @test_server)
      |> IO.inspect(label: :upsert_response)
  end

  defp uni() do
    -1 + 2 * :rand.uniform()
  end

  defp make_vec(size) do
    for i <- 0..size-1 do
      uni()
    end
  end

  def format_ticks(t) do
    div(t, 10000) / 100
  end

  test "5 measure vector insert speed" do
    
    stats = %{}

    {ticks, data} =
      :timer.tc(fn ->
        for i <- 0..@num_test_rows-1 do
          v = make_vec(4)
          %{"id" => :os.system_time(:millisecond), "vector" => v}
          #  |> IO.inspect(label: "test vector")
        end
      end)

    {ticks, resp} =
      :timer.tc(fn ->
        Exqdr.Collection.upsert(data, @test_col_1, @test_server)
      end)

    IO.inspect(format_ticks(ticks), label: "nosync upsert time, #{@num_test_rows} rows")
    IO.inspect(@num_test_rows / format_ticks(ticks), label: "nosync upserts per sec")

    {ticks, resp} =
      :timer.tc(fn ->
        Exqdr.Collection.upsert_wait(data, @test_col_1, @test_server)
      end)

    IO.inspect(format_ticks(ticks), label: "nosync upsert time, #{@num_test_rows} rows")
    IO.inspect(@num_test_rows / format_ticks(ticks), label: "nosync upserts per sec")

    search_tmpl = %{"vector" => [0.5, 0.5, 0.5, 0.5], "top" => 3}

    {ticks, payload} =
      :timer.tc(fn ->
        for i <- 0..@num_queries-1 do
          this_search = search_tmpl |> Map.put("vector", make_vec(4))
          Exqdr.Collection.rank_and_fetch(this_search, @test_col_1, @test_server)
        end
      end)

    IO.inspect(format_ticks(ticks), label: "query time (secs), #{@num_queries} queries")
    IO.inspect(@num_queries / format_ticks(ticks), label: "queries per second")
  end
end
