defmodule Rig.Redis do
  @moduledoc """
  Redis client module for storing and fetching values using Redix.
  """

  use GenServer
  require Logger

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Store Kafka offset for a client_id, topic, event_type, and partition
  """
  def store_offset(client_id, topic, event_type, partition, offset) do
    store_offset(client_id, topic, event_type, partition, offset, nil)
  end

  @doc """
  Store Kafka offset for a client_id, topic, event_type, and partition, with optional TTL (in seconds)
  """
  def store_offset(client_id, topic, event_type, partition, offset, ttl) do
    hash_key = "rig:offsets:#{client_id}"

    IO.inspect({:store_offset, client_id, topic, event_type, partition, offset, ttl}, label: "Rig.Redis.store_offset/7")

    # Convert partition and offset to integers if they're strings
    partition_int =
      case partition do
        partition when is_binary(partition) ->
          case Integer.parse(partition) do
            {int, ""} -> int
            _ -> 0
          end

        partition when is_integer(partition) ->
          partition

        _ ->
          0
      end

    offset_int =
      case offset do
        offset when is_binary(offset) ->
          case Integer.parse(offset) do
            {int, ""} -> int
            _ -> 0
          end

        offset when is_integer(offset) ->
          offset

        _ ->
          0
      end

    # Use the converted partition_int in the field key
    field = "#{topic}:#{event_type}:#{partition_int}"

    IO.inspect({:hset, hash_key, field, offset_int, ttl}, label: "Rig.Redis.store_offset/7 HSET")

    if is_integer(ttl) and ttl > 0 do
      GenServer.call(__MODULE__, {:hset_with_expire, hash_key, field, to_string(offset_int), ttl})
    else
      GenServer.call(__MODULE__, {:hset, hash_key, field, to_string(offset_int)})
    end
  end

  @doc """
  Get Kafka offset for a client_id, topic, event_type, and partition
  """
  def get_offset(client_id, topic, event_type, partition) do
    hash_key = "rig:offsets:#{client_id}"
    field = "#{topic}:#{event_type}:#{partition}"

    IO.inspect({:get_offset, client_id, topic, event_type, partition}, label: "Rig.Redis.get_offset/5")

    case GenServer.call(__MODULE__, {:hget, hash_key, field}) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, ""} ->
        {:ok, nil}

      {:ok, offset} ->
        case Integer.parse(offset) do
          {offset_int, ""} -> {:ok, offset_int}
          _ -> {:error, :invalid_offset}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Get all offsets for a client_id (returns a list of %{topic, event_type, partition, offset})
  """
  def get_all_offsets(client_id) do
    hash_key = "rig:offsets:#{client_id}"

    IO.inspect({:get_all_offsets, client_id}, label: "Rig.Redis.get_all_offsets/1")

    case GenServer.call(__MODULE__, {:hgetall, hash_key}) do
      {:ok, []} ->
        {:ok, []}

      {:ok, fields} ->
        offsets =
          fields
          |> Enum.chunk_every(2)
          |> Enum.filter(fn [key, value] ->
            # Filter out empty keys or values
            key != "" and value != "" and key != nil and value != nil
          end)
          |> Enum.map(fn [key, value] ->
            case String.split(key, ":") do
              [topic, event_type, partition] when topic != "" and event_type != "" and partition != "" ->
                case {Integer.parse(partition), Integer.parse(value)} do
                  {{partition_int, ""}, {offset_int, ""}} ->
                    %{
                      topic: topic,
                      event_type: event_type,
                      partition: partition_int,
                      offset: offset_int
                    }

                  _ ->
                    nil
                end

              _ ->
                nil
            end
          end)
          |> Enum.filter(&(&1 != nil))

        IO.inspect(offsets, label: "Rig.Redis.get_all_offsets/1 result")
        {:ok, offsets}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Delete all offsets for a client_id
  """
  def delete_offsets(client_id) do
    hash_key = "rig:offsets:#{client_id}"
    GenServer.call(__MODULE__, {:del, hash_key})
  end

  @doc """
  Get all offsets for a client_id, topic, and event_type (returns %{partition => offset})
  """
  def get_offsets_for_event_type(client_id, topic, event_type) do
    case get_all_offsets(client_id) do
      {:ok, offsets} ->
        filtered =
          offsets
          |> Enum.filter(fn %{topic: t, event_type: et} -> t == topic and et == event_type end)
          |> Enum.map(fn %{partition: partition, offset: offset} -> {partition, offset} end)
          |> Map.new()

        IO.inspect(filtered, label: "Rig.Redis.get_offsets_for_event_type/3 result")
        {:ok, filtered}

      error ->
        error
    end
  end

  @doc """
  Get all offset information (topic, event_type, partition, offset) for a client_id
  Returns a list of maps with topic, event_type, partition, and offset
  """
  def get_client_offset_info(client_id) do
    hash_key = "rig:offsets:#{client_id}"

    IO.inspect({:get_client_offset_info, client_id}, label: "Rig.Redis.get_client_offset_info/1")

    case GenServer.call(__MODULE__, {:hgetall, hash_key}) do
      {:ok, []} ->
        {:ok, []}

      {:ok, fields} ->
        offset_info =
          fields
          |> Enum.chunk_every(2)
          |> Enum.filter(fn [key, value] ->
            # Filter out empty keys or values
            key != "" and value != "" and key != nil and value != nil
          end)
          |> Enum.map(fn [key, value] ->
            case String.split(key, ":") do
              [topic, event_type, partition] when topic != "" and event_type != "" and partition != "" ->
                case {Integer.parse(partition), Integer.parse(value)} do
                  {{partition_int, ""}, {offset_int, ""}} ->
                    %{
                      topic: topic,
                      event_type: event_type,
                      partition: partition_int,
                      offset: offset_int
                    }

                  _ ->
                    nil
                end

              _ ->
                nil
            end
          end)
          |> Enum.filter(&(&1 != nil))

        IO.inspect(offset_info, label: "Rig.Redis.get_client_offset_info/1 result")
        {:ok, offset_info}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Server Callbacks

  @impl true
  def init(_opts) do
    # Get Redis connection details from config and resolve the values
    config = Application.get_env(:rig, Rig.Redis, [])

    host = resolve_config_value(config[:host], "REDIS_HOST", "localhost")
    port = resolve_config_value(config[:port], "REDIS_PORT", 6379)
    password = resolve_config_value(config[:password], "REDIS_PASSWORD", nil)
    timeout = resolve_config_value(config[:socket_timeout], "REDIS_SOCKET_TIMEOUT", 5000)

    socket_connect_timeout =
      resolve_config_value(config[:socket_connect_timeout], "REDIS_SOCKET_CONNECT_TIMEOUT", 5000)

    ssl = resolve_config_value(config[:ssl], "REDIS_SSL", false)

    # Build connection options
    connection_opts = [
      host: host,
      port: port,
      timeout: timeout,
      ssl: ssl
    ]

    # Add socket_opts if we have a custom connect timeout
    connection_opts =
      if socket_connect_timeout != 5000 do
        Keyword.put(connection_opts, :socket_opts, connect_timeout: socket_connect_timeout)
      else
        connection_opts
      end

    # Add password if provided
    connection_opts =
      if password, do: Keyword.put(connection_opts, :password, password), else: connection_opts

    case Redix.start_link(connection_opts) do
      {:ok, conn} ->
        Logger.info("Connected to Redis at #{host}:#{port}")
        {:ok, %{conn: conn}}

      {:error, reason} ->
        Logger.error("Failed to connect to Redis: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  # Helper function to resolve configuration values
  defp resolve_config_value({:system, env_var, default}, _env_var_name, _fallback) do
    System.get_env(env_var) || default
  end

  defp resolve_config_value({:system, env_var}, env_var_name, fallback) do
    System.get_env(env_var) || System.get_env(env_var_name) || fallback
  end

  defp resolve_config_value(value, _env_var_name, fallback) when is_binary(value) do
    cond do
      is_integer(fallback) ->
        case Integer.parse(value) do
          {int, ""} -> int
          _ -> fallback
        end
      is_boolean(fallback) ->
        case String.downcase(value) do
          "true" -> true
          "false" -> false
          _ -> fallback
        end
      true ->
        value
    end
  end

  defp resolve_config_value(value, _env_var_name, fallback) when is_integer(value) do
    value
  end

  defp resolve_config_value(value, _env_var_name, fallback) when is_boolean(value) do
    value
  end

  defp resolve_config_value(nil, env_var_name, fallback) do
    System.get_env(env_var_name) || fallback
  end

  defp resolve_config_value(_value, _env_var_name, fallback) do
    fallback
  end

  @impl true
  def handle_call({:hset, hash_key, field, value}, _from, %{conn: conn} = state) do
    result = Redix.command(conn, ["HSET", hash_key, field, value])
    {:reply, result, state}
  end

  @impl true
  def handle_call({:hset_with_expire, hash_key, field, value, ttl}, _from, %{conn: conn} = state) do
    result = Redix.command(conn, ["HSET", hash_key, field, value])
    if result == {:ok, _} do
      # Set expiry on the hash key
      _ = Redix.command(conn, ["EXPIRE", hash_key, Integer.to_string(ttl)])
    end
    {:reply, result, state}
  end

  @impl true
  def handle_call({:hget, hash_key, field}, _from, %{conn: conn} = state) do
    case Redix.command(conn, ["HGET", hash_key, field]) do
      {:ok, nil} -> {:reply, {:ok, nil}, state}
      {:ok, value} -> {:reply, {:ok, value}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:hgetall, hash_key}, _from, %{conn: conn} = state) do
    case Redix.command(conn, ["HGETALL", hash_key]) do
      {:ok, fields} -> {:reply, {:ok, fields}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:del, key}, _from, %{conn: conn} = state) do
    result = Redix.command(conn, ["DEL", key])
    {:reply, result, state}
  end

  @impl true
  def handle_info({:redix, _conn, _message}, state) do
    {:noreply, state}
  end
end
