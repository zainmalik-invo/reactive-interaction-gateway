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
      topic = Map.get(event, "topic")

      Rig.Redis.store_offset(
        state.client_id,
        event.type,
        partition,
        offset
      )
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

    stored_offsets =
      Enum.into(offset_info, %{}, fn %{
                                       event_type: event_type,
                                       offset: offset,
                                       partition: partition
                                     } ->
        {event_type, %{offset: offset, partition: partition}}
      end)

    Enum.each(subscriptions, fn %Rig.Subscription{
                                  event_type: et,
                                  constraints: constraints,
                                  start_offset: offset
                                } ->
      {effective_offset, effective_partition} =
        case {offset, Map.get(stored_offsets, et)} do
          {nil, %{offset: stored_offset, partition: stored_partition}}
          when is_integer(stored_offset) ->
            {stored_offset, stored_partition}

          {client_offset, _} when is_integer(client_offset) ->
            {client_offset, 0}

          _ ->
            {nil, 0}
        end

      if effective_offset != nil do
        {:ok, _pid} =
          RigKafka.ReplayKafkaConsumer.start_link(%{
            conn_pid: self(),
            event_type: et,
            constraints: constraints,
            start_offset: effective_offset,
            partition: effective_partition
          })
      else
        live_sub = %Rig.Subscription{
          event_type: et,
          constraints: constraints,
          start_offset: nil
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

    case Map.get(query_map, "rig_redis_client_id") do
      nil ->
        # 2. Try to get from cookies
        cookies = :cowboy_req.parse_cookies(req)

        case cookies do
          {:ok, cookies_list} when is_list(cookies_list) ->
            case Enum.find(cookies_list, fn {key, _value} -> key == "rig_redis_client_id" end) do
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
    client_id = "rig-redis-#{UUID.uuid4()}-#{System.os_time(:millisecond)}"
    Logger.info("Creating and setting client_id: #{client_id}")

    # Don't set cookie here, we'll set it in setup_connection
    {client_id, req}
  end
end
