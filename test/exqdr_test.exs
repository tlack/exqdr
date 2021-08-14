defmodule ExqdrTest do
  use ExUnit.Case
  doctest Exqdr

  @test_server "http://localhost:6333"
  @test_col_1 "test"
  @test_col_2 "testnovec"

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
      resp3 =
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
    for i <- 0..size do
      uni()
    end
  end

  test "5 measure vector insert speed" do
    {ticks, data} =
      :timer.tc(fn ->
        for i <- 0..10_000 do
          v = make_vec(4)
          %{"id" => :os.system_time(:millisecond), "vector" => v}
          #  |> IO.inspect(label: "test vector")
        end
      end)

    IO.inspect({ticks, length(data)}, label: "ROW CREATION TIME !!")

    {ticks, resp} =
      :timer.tc(fn ->
        Exqdr.Collection.upsert(data, @test_col_1, @test_server)
      end)

    IO.inspect({ticks, resp}, label: "UPSERT TIME")
    search3 = %{"vector" => [0.5, 0.5, 0.5, 0.5], "top" => 3}

    {ticks, payload} =
      :timer.tc(fn ->
        for i <- 0..5 do
          this_search = search3 |> Map.put("vector", make_vec(4))
          Exqdr.Collection.rank_and_fetch(this_search, @test_col_1, @test_server)
        end
      end)

    IO.inspect({ticks, payload}, label: "QUERY TIME")
  end
end
