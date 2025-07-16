defmodule RigInboundGatewayWeb.V1.Websocket do
  @moduledoc """
  Cowboy WebSocket handler.
  """

  require Logger

  alias Jason

  alias Result

  alias Rig.EventFilter
  alias RigCloudEvents.CloudEvent
  alias RigInboundGateway.Events
  alias RigInboundGatewayWeb.ConnectionInit
  alias UUID

  @behaviour :cowboy_websocket

  @heartbeat_interval_ms 15_000
  @subscription_refresh_interval_ms 60_000

  # ---

  @impl :cowboy_websocket
  def init(req, :ok) do
    query_params = req |> :cowboy_req.parse_qs() |> Enum.into(%{})
    jwt = query_params["jwt"]

    # Get client_id from cookies
    {client_id, req} = get_client_id(req)
    Logger.info("client_id: #{inspect(client_id)}")

    auth_info =
      case jwt do
        jwt when byte_size(jwt) > 0 ->
          %{auth_header: "Bearer #{jwt}", auth_tokens: [{"bearer", jwt}]}

        _ ->
          nil
      end

    case ConnectionInit.subscriptions_query_param_to_body(query_params) do
      {:ok, encoded_body_or_nil} ->
        request = %{
          auth_info: auth_info,
          query_params: "",
          content_type: "application/json; charset=utf-8",
          body: encoded_body_or_nil,
          client_id: client_id
        }

        state = %{request: request}
        opts = %{idle_timeout: :infinity}
        {:cowboy_websocket, req, state, opts}

      {:error, reason} ->
        req = :cowboy_req.reply(400, %{}, reason, req)
        # Returning :ok at this point simply closes the handler.
        {:ok, req, :unknown_state}
    end
  end

  # ---

  @impl :cowboy_websocket
  def websocket_init(%{request: request}) do
    setup_connection(request)
  end

  def setup_connection(request) do
    on_success = fn subscriptions ->
      # Say "hi", enter the loop and wait for cloud events to forward to the client:
      state = %{subscriptions: subscriptions, client_id: request.client_id}
      {:reply, frame(Events.welcome_event(self(), request.client_id)), state, :hibernate}
    end

    on_error = fn _reason ->
      # WebSocket close frames may include a payload to indicate the error, but we found
      # that error message must be really short; if it isn't, the `{:close, :normal,
      # payload}` is silently converted to `{:close, :abnormal, nil}`. Since there is no
      # limit mentioned in the spec (RFC-6455), we opt for consistent responses,
      # omitting the detailed error.
      reason = "Bad request."
      # This will close the connection:
      {:reply, closing_frame(reason), :no_state}
    end

    ConnectionInit.set_up(
      "WS",
      request,
      on_success,
      on_error,
      @heartbeat_interval_ms,
      @subscription_refresh_interval_ms
    )
  end

  # ---

  # The client may send this as the response to the :ping heartbeat.
  @impl :cowboy_websocket
  def websocket_handle({:pong, _app_data}, state), do: {:ok, state, :hibernate}
  @impl :cowboy_websocket
  def websocket_handle(:pong, state), do: {:ok, state, :hibernate}

  # Allow the client to send :ping messages to test connectivity.
  @impl :cowboy_websocket
  def websocket_handle({:ping, app_data}, _state), do: {:reply, {:pong, app_data}, :hibernate}

  @impl :cowboy_websocket
  def websocket_handle(in_frame, state) do
    Logger.debug(fn -> "Unexpected WebSocket input: #{inspect(in_frame)}" end)
    # This will close the connection:
    {:reply, closing_frame("This WebSocket endpoint cannot be used for two-way communication."),
     state}
  end

  # ---

  @impl :cowboy_websocket
  def websocket_info(:heartbeat, state) do
    # Schedule the next heartbeat:
    Process.send_after(self(), :heartbeat, @heartbeat_interval_ms)
    # Ping the client to keep the connection alive:
    {:reply, :ping, state, :hibernate}
  end

  @impl :cowboy_websocket
  def websocket_info(event, state) when is_struct(event) do
    Logger.debug(fn -> "event in websocket: " <> inspect(event) end)

    if event.extensions != %{} do
      partition = Map.get(event.extensions, "x-kafka-partition")
      offset = Map.get(event.extensions, "x-kafka-offset")
      topic =
        cond do
          Map.has_key?(event, :topic) -> Map.get(event, :topic)
          Map.has_key?(event, "topic") -> Map.get(event, "topic")
          true -> "rig"
        end

      # Only store offset if enable_replay is true for this event_type
      sub =
        case state[:subscriptions] do
          subs when is_list(subs) ->
            Enum.find(subs, fn sub -> sub.event_type == event.type end)
          _ -> nil
        end

      enable_replay? = sub && Map.get(sub, :enable_replay, false)

      if enable_replay? do
        # Determine effective TTL
        max_cache_ttl = Application.get_env(:rig, :max_cache_ttl, 604800)
        sub_ttl = Map.get(sub, :cache_ttl)
        effective_ttl =
          cond do
            is_integer(sub_ttl) and sub_ttl > 0 -> min(sub_ttl, max_cache_ttl)
            true -> max_cache_ttl
          end

        IO.inspect({:store_offset, state.client_id, topic, event.type, partition, offset, effective_ttl}, label: "WS.store_offset")
        Rig.Redis.store_offset(
          state.client_id,
          topic,
          event.type,
          partition,
          offset,
          effective_ttl
        )
      end
    end

    # Forward the event to the client:
    {:reply, frame(event), state, :hibernate}
  end

  @impl :cowboy_websocket
  def websocket_info({:set_subscriptions, subscriptions}, state) do
    Logger.debug(fn -> "subscriptions: #{inspect(subscriptions)}" end)

    # Send subscriptions_set event to client
    send_frame = frame(Events.subscriptions_set(subscriptions))
    new_state = Map.put(state, :subscriptions, subscriptions)

    # Fetch current offsets before processing new subscriptions
    {:ok, offset_info} = Rig.Redis.get_client_offset_info(state.client_id)
    IO.inspect(offset_info, label: "WS.set_subscriptions offset_info")

    # Build a map of {{topic, event_type, partition} => offset}
    stored_offsets =
      Enum.reduce(offset_info, %{}, fn %{
                                         topic: topic,
                                         event_type: event_type,
                                         offset: offset,
                                         partition: partition
                                       }, acc ->
        Map.put(acc, {topic, event_type, partition}, offset)
      end)

    IO.inspect(stored_offsets, label: "WS.set_subscriptions stored_offsets")

    Enum.each(subscriptions, fn %Rig.Subscription{
                                  event_type: et,
                                  constraints: constraints,
                                  start_offset: offset,
                                  enable_replay: enable_replay
                                } ->
      if enable_replay do
        # Find all (topic, partition) for this event_type
        partitions_for_type =
          stored_offsets
          |> Enum.filter(fn {{topic, event_type, _partition}, _offset} -> event_type == et end)

        if partitions_for_type != [] do
          Enum.each(partitions_for_type, fn {{topic, _event_type, partition}, stored_offset} ->
            effective_offset =
              case offset do
                nil -> stored_offset + 1
                client_offset when is_integer(client_offset) -> client_offset + 1
                _ -> nil
              end

            if effective_offset != nil do
              IO.inspect({:replay_consumer, topic, et, partition, effective_offset}, label: "WS.set_subscriptions starting replay")
              {:ok, _pid} =
                RigKafka.ReplayKafkaConsumer.start_link(%{
                  conn_pid: self(),
                  topic: topic,
                  event_type: et,
                  constraints: constraints,
                  start_offset: effective_offset,
                  partition: partition
                })
            end
          end)
        else
          # No stored offsets, but enable_replay is true, so just refresh subscriptions as live
          live_sub = %Rig.Subscription{
            event_type: et,
            constraints: constraints,
            start_offset: nil,
            enable_replay: true
          }

          EventFilter.refresh_subscriptions([live_sub], [])
        end
      else
        # enable_replay is false, always treat as live subscription (no replay)
        live_sub = %Rig.Subscription{
          event_type: et,
          constraints: constraints,
          start_offset: nil,
          enable_replay: false
        }

        EventFilter.refresh_subscriptions([live_sub], [])
      end
    end)

    {:reply, send_frame, new_state, :hibernate}
  end

  @impl :cowboy_websocket
  def websocket_info(:refresh_subscriptions, state) do
    EventFilter.refresh_subscriptions(state.subscriptions, [])
    Process.send_after(self(), :refresh_subscriptions, @subscription_refresh_interval_ms)
    {:ok, state}
  end

  @impl :cowboy_websocket
  def websocket_info({:session_killed, session_id}, state) do
    Logger.info("Session killed: #{inspect(session_id)} - terminating WS/#{inspect(self())}..")
    # This will close the connection:
    {:reply, closing_frame("Session killed."), state}
  end

  # ---

  @impl :cowboy_websocket
  def terminate(reason, _req, state) do
    Logger.debug(fn ->
      pid = inspect(self())
      reason = "reason=" <> inspect(reason)
      "Closing WebSocket connection (#{pid}, #{reason})"
    end)

    if is_map(state) and Map.has_key?(state, :client_id) do
      Rig.Redis.delete_offsets(state.client_id)
    end

    :ok
  end

  # ---

  defp frame(%CloudEvent{json: json}) do
    {:text, json}
  end

  defp frame(event) do
    {:text, Cloudevents.to_json(event)}
  end

  # ---

  defp closing_frame(reason) do
    # Sending this will close the connection:
    {
      :close,
      # "Normal Closure":
      1_000,
      reason
    }
  end

  # ---

  defp get_client_id(req) do
    # 1. Try to get from query params
    query_params = :cowboy_req.parse_qs(req)
    query_map = Enum.into(query_params, %{})

    case Map.get(query_map, "replay_token") do
      nil ->
        # 2. Try to get from cookies
        cookies = :cowboy_req.parse_cookies(req)

        case cookies do
          {:ok, cookies_list} when is_list(cookies_list) ->
            case Enum.find(cookies_list, fn {key, _value} -> key == "replay_token" end) do
              {client_id, _value} -> {client_id, req}
              _ -> create_and_set_client_id(req)
            end

          _ ->
            create_and_set_client_id(req)
        end

      client_id ->
        {client_id, req}
    end
  end

  defp create_and_set_client_id(req) do
    client_id = "replay-token-#{UUID.uuid4()}-#{System.os_time(:millisecond)}"
    Logger.info("Creating and setting replay_token: #{client_id}")

    # Don't set cookie here, we'll set it in setup_connection
    {client_id, req}
  end
end
