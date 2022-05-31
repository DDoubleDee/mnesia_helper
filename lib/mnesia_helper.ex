defmodule MnesiaHelper do
  @moduledoc """
  This module simplifies :mnesia, and works well with Ecto schemas.

  It's recommended to surround your code in a try catch as all of the functions will throw up any errors that occur.

  All of the errors that will be thrown will look like {:error, "message", extra_data, error_code}.

  To use this module, you MUST first use the init!() function, even if you already created a schema.

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
        GenServer.start_link(MnesiaHelperCounter, [], name: :counter)
        start()
      {:error, {_, {:already_exists, _}}} ->
        GenServer.start_link(MnesiaHelperCounter, [], name: :counter)
        start()
      error ->
        throw create_error(0, "Unexpected error", error)
    end
  end
  ###############################################################
  @doc """
  This function creates (deletes if `:already_exists`) a table with a `name`, with all of the keys in `attributes_map`.

  `attributes_map` can either be a normal map, or an Ecto schema (Your map/schema must at least have two keys, one of them always being `:id` (which must always be `nil`)).

  If keys `:created_at` or `:updated_at` are present in `attributes_map`, they are automatically updated, and should almost always (see docs for update!()) be `nil`.

  Any extra indexes need to be listed as atoms in `index_list`. Any other options should be passed to `extra_opts` as a keylist (the same way you would normally pass it to :mnesia.create_table()).

  This function returns `:ok`.

  If any mistake (except `:already_exists`) occurs, this function will throw an error.

  ## Examples

      iex> create_table!(:people, %{id: nil, name: nil, age: nil, created_at: nil, updated_at: nil})
      :ok

  """
  @spec create_table!(any, map(), [atom()], [{atom(), list()}]) :: :ok
  def create_table!(name, attributes_map, index_list \\ [], extra_opts \\ [])

  def create_table!(name, attributes_struct, index_list, extras) when is_struct(attributes_struct) do
    create_table!(name, to_map(attributes_struct), index_list, extras)
  end

  def create_table!(name, attributes_map, index_list, extras) do
    :mnesia.create_table(name, [{:attributes_map, [:id | List.delete(Enum.sort(Map.keys(attributes_map)), :id)]} | [{:index, index_list} | extras]])
    |> case do
      {:atomic, :ok} ->
        :ok
      {:aborted, {:already_exists, _}} ->
        :mnesia.delete_table(name)
        GenServer.cast(:counter, {name, 0})
        create_table!(name, attributes_map, index_list)
      {:aborted, error} ->
        throw create_error(1002, "Mnesia error", error)
      error ->
        throw create_error(0, "Unexpected error", error)
    end
  end
  ###############################################################
  @doc """
  This function writes all values in map/struct `input` into a table named `name`.

  Keys `:created_at`, `:updated_at` and `:id` are always ignored as they are automatically set if they are present in table attributes.

  This function returns `:ok`.

  If any mistake occurs, this function will throw an error.

  ## Examples

      iex> write!(:people, %{name: "John", age: 21})
      :ok

  """
  @spec write!(any, map()) :: :ok
  def write!(name, input)

  def write!(name, input) when is_struct(input) do
    write!(name, to_map(input))
  end

  def write!(name, input) when not is_map(input) do
    throw create_error(1004, "Input must be a map or an ecto struct(prefer ecto structs)", {name, input})
  end

  def write!(name, input) do
    keys = List.delete(Enum.sort(get_keys!(name)), :id)
    input = Enum.reduce(keys, {name, GenServer.call(:counter, name)}, fn
      :created_at, acc ->
        Tuple.append(acc, get_time())
      :updated_at, acc ->
        Tuple.append(acc, get_time())
      key, acc ->
        case Map.get(input, key, nil) do
          nil ->
            throw create_error(1003, "Missing key", {input, key})
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
      {:aborted, error} ->
        throw create_error(1002, "Mnesia error", error)
      error ->
        throw create_error(0, "Unexpected error", error)
      end
  end
  ###############################################################
  @doc """
  This function overwrites all values under key `:id` == `id` inside the table named `name` by `input`.

  IMPORTANT: If key `:created_at` is present in the table attributes, it should be set in `input`, as otherwise the function will have to pull this value from the original record, which INCREASES time to execute this function by about 1.5 times.

  This function returns `:ok`.

  Key `:updated_at` is always ignored as it is automatically set if it is present in table attributes.

  If any mistake occurs, this function will throw an error.

  ## Examples

      iex> update!(:people, %{name: "John", age: 25}, 0)
      :ok

  """
  @spec update!(any, map(), integer()) :: :ok
  def update!(name, input, id)

  def update!(name, input, id) when is_struct(input) do
    update!(name, to_map(input), id)
  end

  def update!(name, input, _) when not is_map(input) do
    throw create_error(1004, "Input must be a map or an ecto struct(prefer ecto structs)", {name, input})
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
        Tuple.append(acc, get_time())
      key, acc ->
        case Map.get(input, key, nil) do
          nil ->
            throw create_error(1003, "Missing key", {input, key})
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
      {:aborted, error} ->
        throw create_error(1002, "Mnesia error", error)
      error ->
        throw create_error(0, "Unexpected error", error)
      end
  end
  ###############################################################
  @doc """
  This function matches the given map/structure `input` with any records that exist in table named `name`.

  This function returns a list of maps.

  If there are no records that match, it returns an empty list.

  If any mistake occurs, this function will throw an error.

  ## Examples

      iex> match!(:people, %{name: "John"})
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
    throw create_error(1004, "Input must be a map or an ecto struct(prefer ecto structs)", {name, input})
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
      {:aborted, error} ->
        throw create_error(1002, "Mnesia error", error)
      error ->
        throw create_error(0, "Unexpected error", error)
    end
  end
  ###############################################################
  @doc """
  This function returns all records from a table named `name` where `index` == `input`.

  This function returns a list of maps.

  If there are no records that match, it returns an empty list.

  If any mistake occurs, this function will throw an error.

  ## Examples

      iex> index_read!(:people, 0, :id)
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
      {:aborted, error} ->
        throw create_error(1002, "Mnesia error", error)
      error ->
        throw create_error(0, "Unexpected error", error)
    end
  end

  def index_read!(name, input, index) do
    :mnesia.transaction(fn ->
      :mnesia.index_read(name, input, index)
    end)
    |> case do
      {:atomic, out} when is_list(out) ->
        create_output(out, [:id | List.delete(Enum.sort(get_keys!(name)), :id)])
      {:aborted, error} ->
        throw create_error(1002, "Mnesia error", error)
      error ->
        throw create_error(0, "Unexpected error", error)
    end
  end
  ###############################################################
  @doc """
  This function selects any records from a table named `name` where all `guards` are true.

  All `guards` are the same as in a regular :mnesia.select(), however, instead of an ambiguous lambda symbol, you can use any keys that are inside this table's attributes.

  This function returns a list of maps.

  If there are no records that match, it returns an empty list.

  If any mistake occurs, this function will throw an error.

  ## Examples

      iex> select_all!(:people, [{:>, :age, 21}])
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
  @spec select_all!(any, [{atom(), atom(), any}]) :: [map()]
  def select_all!(name, guards)

  def select_all!(name, guards) do
    keys = [:id | List.delete(Enum.sort(get_keys!(name)), :id)]
    count = Enum.count(keys) - 1
    input = Enum.reduce(0..count, {name}, fn
      index, acc ->
        Tuple.append(acc, String.to_atom("$" <> to_string(index + 1)))
    end)
    guards = Enum.map(guards, fn {operation, key, value} ->
      {operation, String.to_atom("$" <> to_string(1 + Enum.find_index(keys, fn kei -> kei == key end))), value}
    end)
    [{input, guards, [:"$$"]}]
    :mnesia.transaction(fn ->
      :mnesia.select(name, [{input, guards, [:"$$"]}])
    end)
    |> case do
      {:atomic, out} when is_list(out) ->
        create_output(out, keys)
      error ->
        throw create_error(0, "Unexpected error", error)
    end
  end
  ###############################################################
  @doc """
  This function returns a list of all keys that are present inside a table named `name`.

  This function returns a list of atoms.

  If any mistake occurs, this function will throw an error.

  ## Examples

      iex> get_keys!(:people)
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
        throw create_error(0, "Unexpected error", error)
    end
  end

  ###############################################################
  #Private fns
  ###############################################################

  defp create_error(code, msg, error) do
    {:error, msg, error, code}
  end

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

  defp get_time() do
    DateTime.utc_now()
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
        throw create_error(0, "Unexpected error", error)
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
