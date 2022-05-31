defmodule MnesiaHelperTest do
  use ExUnit.Case
  doctest MnesiaHelper

  test "general debug" do
    init!()
    create_table!(:test, %{id: nil, test: nil, test2: nil, test3: nil, created_at: nil, updated_at: nil}, [])
    write!(:test, %{id: nil, test: "lol", test2: 4, test3: "test1", created_at: nil, updated_at: nil})
    IO.inspect select_all!(:test, [{:>, :test2, 3}])
    write!(:test, %{id: nil, test: "haha", test2: 2, test3: "test2", created_at: nil, updated_at: nil})
    IO.inspect match!(:test, %{test: "haha"})
    :timer.sleep(1000)
    update!(:test, %{id: nil, test: "lool", test2: 5, test3: "test1", created_at: nil, updated_at: nil}, 0)
    #IO.inspect index_read!(:test, "test1", :test3)
    start = get_time()
    Enum.each(0..100000, fn _ ->
      update!(:test, %{id: nil, test: "haha", test2: 2, test3: "test2", created_at: start, updated_at: nil}, 0)
    end)
    done = get_time()
    IO.inspect DateTime.diff(start, done, :microsecond)
    IO.inspect index_read!(:test, 0, :id)
    :ok
  end
end
