defmodule MnesiaHelper do
  @moduledoc """
  This module simplifies :mnesia, and works well with Ecto schemas.

  It's recommended to surround your code in a try catch as all of the functions will throw up any errors that occur.

  All of the (non-mnesia) errors that will be thrown will look like {:error, "message", extra_data}.

  To use this module, you MUST first use the init!() function, even if you already created a schema.

  You might want to change the function that automatically sets the time to a format you prefer, by default it's DateTime.utc_now().

  After that, you're free to use the rest of the module.
  """
  ###############################################################
  @doc """
  This function creates a schema using the passed `nodes`, starts mnesia and returns `:ok`.

  If any mistake (except `:already_exists`) occurs, this function will throw an error.

  ## Examples

      iex> init!()
      :ok

  """
  @spec init!(list()) :: :ok
  def init!(nodes \\ [node()])

  def init!(nodes) do
    :mnesia.create_schema(nodes)
    |> case do
      :ok ->
        GenServer.start_link(MnesiaHelperCounter, [], name: :mhcounter)
        GenServer.start_link(MnesiaHelperTimer, [], name: :mhtimer)
        start()
      {:error, {_, {:already_exists, _}}} ->
        GenServer.start_link(MnesiaHelperCounter, [], name: :mhcounter)
        GenServer.start_link(MnesiaHelperTimer, [], name: :mhtimer)
        start()
      error ->
        throw error
    end
  end
  ###############################################################
  @doc """
  This function creates (deletes if `:already_exists`) a table with a `name`, with all of the keys in `attributes_map`.

  `attributes_map` can either be a list, a map, or an Ecto schema (struct) (Your list/map/schema doesn't need to have `:id`, as this function adds it anyway, but you need at least one other key or mnesia will complain)).

  If keys `:created_at` or `:updated_at` are present in `attributes_map`, they are automatically updated, and should almost always (see docs for `MnesiaHelper.update!()`) be `nil`.

  Any extra indexes need to be listed as atoms in `index_list`. Any other options should be passed to `extra_opts` as a keylist (the same way you would normally pass it to :mnesia.create_table()).

  This function returns `:ok`.

  If any mistake (except `:already_exists`) occurs, this function will throw an error.

  ## Examples

  Here, we create a table named `:people` with attributes `[:id, :name, :age, :created_at, :updated_at]`:

      iex> MnesiaHelper.create_table!(:people, [:id, :name, :age, :created_at, :updated_at])
      :ok

  We can also use a map:

      iex> MnesiaHelper.create_table!(:people, %{
        id: nil, name: nil, age: nil, created_at: nil, updated_at: nil})
      :ok

  Or a struct (I won't repeat myself after this, a struct can go in all places where maps can go):

      iex> MnesiaHelper.create_table!(:people, %Person{})
      :ok

  """
  @spec create_table!(any, map(), [atom()], [{atom(), list()}]) :: :ok
  def create_table!(name, attributes_map, index_list \\ [], extra_opts \\ [])

  def create_table!(name, attributes_struct, index_list, extras) when is_struct(attributes_struct) do
    create_table!(name, to_map(attributes_struct), index_list, extras)
  end

  def create_table!(name, attributes_list, index_list, extras) when is_list(attributes_list) do
    create_table!(name, to_map(attributes_list), index_list, extras)
  end

  def create_table!(name, attributes_map, index_list, extras) do
    :mnesia.create_table(name, [{:attributes, [:id | List.delete(Enum.sort(Map.keys(attributes_map)), :id)]} | [{:index, index_list} | extras]])
    |> case do
      {:atomic, :ok} ->
        :ok
      {:aborted, {:already_exists, _}} ->
        :mnesia.delete_table(name)
        GenServer.cast(:mhcounter, {name, 0})
        create_table!(name, attributes_map, index_list)
      error ->
        throw error
    end
  end
  ###############################################################
  @doc """
  This function writes all values in map/struct `input` into a table named `name`.

  Keys `:created_at`, `:updated_at` and `:id` are always ignored as they are automatically set if they are present in table attributes.

  This function returns `:ok`.

  If any mistake occurs, this function will throw an error.

  ## Examples

  Here, we write a record %{name: "John", age: 21} into a table named `:people`:

      iex> MnesiaHelper.write!(:people, %{name: "John", age: 21})
      :ok
  """
  @spec write!(any, map()) :: :ok
  def write!(name, input)

  def write!(name, input) when is_struct(input) do
    write!(name, to_map(input))
  end

  def write!(name, input) when not is_map(input) do
    throw {:error, "Input must be a map or an ecto struct(prefer ecto structs)", {name, input}}
  end

  def write!(name, input) do
    keys = List.delete(Enum.sort(get_keys!(name)), :id)
    input = Enum.reduce(keys, {name, GenServer.call(:mhcounter, name)}, fn
      :created_at, acc ->
        Tuple.append(acc, GenServer.call(:mhtimer, 0))
      :updated_at, acc ->
        Tuple.append(acc, GenServer.call(:mhtimer, 0))
      key, acc ->
        case Map.get(input, key, :no_key) do
          :no_key ->
            throw {:error, "Missing key", {input, key}}
          value ->
            Tuple.append(acc, value)
        end
    end)
    :mnesia.transaction(fn ->
      :mnesia.write(input)
    end)
    |> case do
      {:atomic, :ok} ->
        :ok
      error ->
        throw error
      end
  end
  ###############################################################
  @doc """
  This function overwrites all values under key `:id` == `id` inside the table named `name` by `input`.

  IMPORTANT: If key `:created_at` is present in the table attributes, it should be set in `input`, as otherwise the function will have to pull this value from the original record, which multiplies time to execute this function by about 1.5 times.

  This function returns `:ok`.

  Key `:updated_at` is always ignored as it is automatically set if it is present in table attributes.

  If any mistake occurs, this function will throw an error.

  ## Examples

  Here, we update the record where `:id` is 0 in a table `people`:

      iex> MnesiaHelper.update!(:people, %{name: "John", age: 25}, 0)
      :ok

  """
  @spec update!(any, map(), integer()) :: :ok
  def update!(name, input, id)

  def update!(name, input, id) when is_struct(input) do
    update!(name, to_map(input), id)
  end

  def update!(name, input, _) when not is_map(input) do
    throw {:error, "Input must be a map or an ecto struct(prefer ecto structs)", {name, input}}
  end

  def update!(name, input, id) do
    keys = List.delete(Enum.sort(get_keys!(name)), :id)
    input = Enum.reduce(keys, {name, id}, fn
      :created_at, acc ->
        out = if is_nil(Map.get(input, :created_at, nil)) do
          [before | _] = index_read!(name, id, :id)
          before
        else
          input
        end
        Tuple.append(acc, Map.get(out, :created_at))
      :updated_at, acc ->
        Tuple.append(acc, GenServer.call(:mhtimer, 0))
      key, acc ->
        case Map.get(input, key, :no_key) do
          :no_key ->
            throw {:error, "Missing key", {input, key}}
          value ->
            Tuple.append(acc, value)
        end
    end)
    :mnesia.transaction(fn ->
      :mnesia.write(input)
    end)
    |> case do
      {:atomic, :ok} ->
        :ok
      error ->
        throw error
      end
  end
  ###############################################################
  @doc """
  This function matches the given map/structure `input` with any records that exist in table named `name`.

  This function returns a list of maps.

  If there are no records that match, it returns an empty list.

  If any mistake occurs, this function will throw an error.

  ## Examples

  Here, we find all records that have `"John"` under their `:name` key in table `:people`:

      iex> MnesiaHelper.match!(:people, %{name: "John"})
      [
        %{
          age: 25,
          created_at: ~U[2022-05-31 17:56:38.658000Z],
          id: 0,
          name: "John",
          updated_at: ~U[2022-05-31 17:57:19.492000Z]
        }
      ]

  """
  @spec match!(any, map()) :: [map()]
  def match!(name, input)

  def match!(name, input) when is_struct(input) do
    match!(name, to_map(input))
  end

  def match!(name, input) when not is_map(input) do
    throw {:error, "Input must be a map or an ecto struct(prefer ecto structs)", {name, input}}
  end

  def match!(name, input) do
    keys = [:id | List.delete(Enum.sort(get_keys!(name)), :id)]
    input = Enum.reduce(keys, {name}, fn
      key, acc ->
        case Map.get(input, key, :_) do
          nil ->
            Tuple.append(acc, :_)
          i ->
            Tuple.append(acc, i)
        end
    end)
    :mnesia.transaction(fn ->
      :mnesia.match_object(input)
    end)
    |> case do
      {:atomic, out} when is_list(out) ->
        create_output(out, keys)
      error ->
        throw error
    end
  end
  ###############################################################
  @doc """
  This function returns all records from a table named `name` where `index` == `input`.

  This function returns a list of maps.

  If there are no records that match, it returns an empty list.

  If any mistake occurs, this function will throw an error.

  ## Examples

  Here, we find all records where `:id` is 0 in table `:people`:

      iex> MnesiaHelper.index_read!(:people, 0, :id)
      [
        %{
          age: 25,
          created_at: ~U[2022-05-31 17:56:38.658000Z],
          id: 0,
          name: "John",
          updated_at: ~U[2022-05-31 17:57:19.492000Z]
        }
      ]

  """
  @spec index_read!(any, any, atom()) :: [map()]
  def index_read!(name, input, index)

  def index_read!(name, input, :id) do
    :mnesia.transaction(fn ->
      :mnesia.read({name, input})
    end)
    |> case do
      {:atomic, out} when is_list(out) ->
        create_output(out, [:id | List.delete(Enum.sort(get_keys!(name)), :id)])
      error ->
        throw error
    end
  end

  def index_read!(name, input, index) do
    :mnesia.transaction(fn ->
      :mnesia.index_read(name, input, index)
    end)
    |> case do
      {:atomic, out} when is_list(out) ->
        create_output(out, [:id | List.delete(Enum.sort(get_keys!(name)), :id)])
      error ->
        throw error
    end
  end
  ###############################################################
  @doc """
  This function selects any records from a table named `name` where all `guards` are true and returns only the keys in `columns`.

  All `guards` are the same as in a regular :mnesia.select(), however, instead of some ambiguous symbol, you can use any keys that are inside this table's attributes.

  By default `columns` is empty and returns every column in record.

  This function returns a list of maps.

  If there are no records that match, it returns an empty list.

  If any mistake occurs, this function will throw an error.

  ## Examples

  Here, we fetch all records where `:age` is bigger than 21, outputting all columns in table `:people`:

      iex> MnesiaHelper.select!(:people, [{:>, :age, 21}])
      [
        %{
          age: 25,
          created_at: ~U[2022-05-31 17:56:38.658000Z],
          id: 0,
          name: "John",
          updated_at: ~U[2022-05-31 17:57:19.492000Z]
        }
      ]

  Here, we fetch all records where `:age` is bigger than 21, but only output keys `:age`, `:name` and `:created_at` in table `:people`:

      iex> MnesiaHelper.select!(:people, [{:>, :age, 21}], [:age, :name, :created_at])
      [%{age: 25, created_at: ~U[2022-05-31 17:56:38.658000Z], name: "John"}]

  """
  @spec select!(any, [{atom(), atom(), any}], [atom()]) :: [map()]
  def select!(name, guards, columns \\ [])

  def select!(name, guards, columns) do
    keys = [:id | List.delete(Enum.sort(get_keys!(name)), :id)]
    count = Enum.count(keys) - 1
    input = Enum.reduce(0..count, {name}, fn
      index, acc ->
        Tuple.append(acc, String.to_atom("$" <> to_string(index + 1)))
    end)
    guards = Enum.map(guards, fn {operation, key, value} ->
      {operation, String.to_atom("$" <> to_string(1 + Enum.find_index(keys, fn kei -> kei == key end))), value}
    end)
    column_list = Enum.map(columns, fn item ->
      String.to_atom("$" <> to_string(1 + Enum.find_index(keys, fn kei -> kei == item end)))
    end)
    :mnesia.transaction(fn ->
      :mnesia.select(name, [{input, guards, Enum.sort(case column_list do [] -> [:"$$"]; x -> [x] end)}])
    end)
    |> case do
      {:atomic, out} when is_list(out) ->
        Enum.map(out, fn
          item when is_list(item) ->
            item
          item ->
            [item]
        end)
        |> create_output(
        case columns do
          [] -> keys;
          x -> x
        end)
      error ->
        throw error
    end
  end
  ###############################################################
  @doc """
  This function returns a list of all keys that are present inside a table named `name`.

  This function returns a list of atoms.

  If any mistake occurs, this function will throw an error.

  ## Examples

  Here, we get a list of all keys in table `:people`:

      iex> MnesiaHelper.get_keys!(:people)
      [:id, :age, :created_at, :name, :updated_at]

  """
  @spec get_keys!(any) :: [atom()]
  def get_keys!(name)

  def get_keys!(name) do
    :mnesia.table_info(name, :attributes)
    |> case do
      out when is_list(out) ->
        out
      error ->
        throw error
    end
  end

  @doc """
    Use this function to set a function that will determine the datetime that is set in `:created_at` and `:updated_at`.

    By default that function is `fn -> DateTime.utc_now() end`.

  ## Examples

  Here, we change the format of the date to iso8601, and this will apply automatically to all writes and updates in the future:

      iex> MnesiaHelper.set_time_fn(fn -> DateTime.utc_now() |> DateTime.to_iso8601() end)
      :ok

  Here is an example of how that looks:

      iex> MnesiaHelper.index_read!(:people, 0, :id)
      [
        %{
          age: 21,
          created_at: "2022-06-02T10:47:50.532000Z",
          id: 0,
          name: "John",
          updated_at: "2022-06-02T10:47:50.540000Z"
        }
      ]

  """
  @spec set_time_fn(function()) :: :ok
  def set_time_fn(function) do
    GenServer.cast(:mhtimer, function)
  end

  ###############################################################
  #Private fns
  ###############################################################

  #parses a standard mnesia record output into a normal map
  defp create_output([check | _] = input, keys) when is_tuple(check) do
    Enum.map(input, fn item ->
      item = Tuple.delete_at(item, 0)
      Enum.reduce(0..Enum.count(keys) - 1, %{}, fn index, acc ->
        Map.put(acc, Enum.at(keys, index), elem(item, index))
      end)
    end)
  end

  defp create_output(input, keys) do
    Enum.map(input, fn item ->
      Enum.reduce(0..Enum.count(keys) - 1, %{}, fn index, map ->
        Map.put(map, Enum.at(keys, index), Enum.at(item, index))
      end)
    end)
  end

  defp to_map(input) when is_list(input) do
    Enum.reduce(input, %{}, fn item, acc-> Map.put(acc, item, nil) end)
  end

  defp to_map(input) do
    Map.from_struct(input)
    |> Map.delete(:__meta__)
  end

  defp start() do
    :mnesia.start()
    |> case do
      :ok ->
        :ok
      error ->
        throw error
    end
  end

end

defmodule MnesiaHelperCounter do
  @moduledoc false
  use GenServer

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call(name, _, state) do
    {:reply, Map.get(state, name, 0), Map.put(state, name, Map.get(state, name, 0) + 1)}
  end

  @impl true
  def handle_cast({name, id}, state) do
    {:noreply, Map.put(state, name, id)}
  end

end

defmodule MnesiaHelperTimer do
  @moduledoc false
  use GenServer

  @impl true
  def init(_) do
    {:ok, fn -> DateTime.utc_now() end}
  end

  @impl true
  def handle_call(_, _, state) do
    {:reply, state.(), state}
  end

  @impl true
  def handle_cast(func, _) do
    {:noreply, func}
  end
end
