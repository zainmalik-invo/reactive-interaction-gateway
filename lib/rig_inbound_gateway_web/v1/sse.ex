defmodule RigInboundGatewayWeb.V1.SSE do
  @moduledoc """
  Server-Sent Events (SSE) handler.
  """
  @behaviour :cowboy_loop

  use Rig.Config, [:cors]

  alias Jason
  alias ServerSentEvent

  alias Result

  alias Rig.EventFilter
  alias RigCloudEvents.CloudEvent
  alias RigInboundGateway.Events
  alias RigInboundGatewayWeb.ConnectionInit

  require Logger

  @heartbeat_interval_ms 15_000
  @subscription_refresh_interval_ms 60_000

  @impl :cowboy_loop
  def init(req, _state) do
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

        setup_connection(req, request)

      {:error, reason} ->
        req = :cowboy_req.reply(400, %{}, reason, req)
        # Returning :ok at this point simply closes the handler.
        {:ok, req, :unknown_state}
    end
  end

  # ---

  def setup_connection(req, request) do
    conf = config()

    on_success = fn subscriptions ->
      # Tell the client the request is good and the response is chunked:
      headers = %{
        "content-type" => "text/event-stream; charset=utf-8",
        "cache-control" => "no-cache",
        "access-control-allow-origin" => conf.cors
      }

      req = :cowboy_req.stream_reply(200, headers, req)

      # Say hello to the client:
      Events.welcome_event(self(), request.client_id)
      |> to_server_sent_event()
      |> send_via(req)

      # Fetch stored offsets from Redis using client_id
      case Rig.Redis.get_client_offset_info(request.client_id) do
        {:ok, offset_info} when offset_info != [] ->
          Logger.info(
            "Found stored offsets for client #{request.client_id}: #{inspect(offset_info)}"
          )

          # Convert to the format expected by the rest of the code
          stored_offsets =
            Enum.into(offset_info, %{}, fn %{
                                             event_type: event_type,
                                             offset: offset,
                                             partition: partition
                                           } ->
              {event_type, %{offset: offset, partition: partition}}
            end)

          # Merge stored offsets with subscriptions
          subscriptions_with_offsets =
            Enum.map(subscriptions, fn subscription ->
              event_type = subscription.event_type

              case Map.get(stored_offsets, event_type) do
                nil ->
                  subscription

                %{offset: offset, partition: partition}
                when is_integer(offset) and is_integer(partition) ->
                  # Don't start replay consumer here, it will be started in set_subscriptions
                  %{subscription | start_offset: offset}
              end
            end)

          state = %{
            subscriptions: subscriptions_with_offsets,
            client_id: request.client_id
          }

          # Initialize event filter with merged subscriptions
          EventFilter.refresh_subscriptions(subscriptions_with_offsets, [])
          Process.send_after(self(), :refresh_subscriptions, @subscription_refresh_interval_ms)

          {:cowboy_loop, req, state, :hibernate}

        _ ->
          state = %{
            subscriptions: subscriptions,
            client_id: request.client_id
          }

          # Initialize event filter with original subscriptions
          EventFilter.refresh_subscriptions(subscriptions, [])
          Process.send_after(self(), :refresh_subscriptions, @subscription_refresh_interval_ms)

          {:cowboy_loop, req, state, :hibernate}
      end
    end

    on_error = fn reason ->
      req =
        case reason do
          {code, reason} -> :cowboy_req.reply(code, %{}, reason, req)
          _ -> :cowboy_req.reply(400, %{}, reason, req)
        end

      # Returning :ok at this point simply closes the handler.
      {:ok, req, :no_state}
    end

    ConnectionInit.set_up(
      "SSE",
      request,
      on_success,
      on_error,
      @heartbeat_interval_ms,
      @subscription_refresh_interval_ms
    )
  end

  # ---

  @impl :cowboy_loop
  def info(:heartbeat, req, state) do
    # We send a heartbeat now:
    :heartbeat
    |> to_server_sent_event()
    |> send_via(req)

    # And schedule the next one:
    Process.send_after(self(), :heartbeat, @heartbeat_interval_ms)

    {:ok, req, state, :hibernate}
  end

  @impl :cowboy_loop
  def info(event, req, state) when is_struct(event) do
    Logger.debug(fn -> "event in sse: " <> inspect(event) end)

    if event.extensions != %{} do
      {client_id, _req} = get_client_id(req)
      partition = Map.get(event.extensions, "x-kafka-partition")
      offset = Map.get(event.extensions, "x-kafka-offset")
      topic =
        cond do
          Map.has_key?(event, :topic) -> Map.get(event, :topic)
          Map.has_key?(event, "topic") -> Map.get(event, "topic")
          true -> "rig"
        end

      IO.inspect({:store_offset, client_id, topic, event.type, partition, offset}, label: "SSE.store_offset")
      Rig.Redis.store_offset(
        client_id,
        topic,
        event.type,
        partition,
        offset
      )
    end

    # Forward the event to the client:
    event
    |> to_server_sent_event()
    |> send_via(req)

    {:ok, req, state, :hibernate}
  end

  @impl :cowboy_loop
  def info({:set_subscriptions, subscriptions}, req, state) do
    Logger.debug(fn -> "subscriptions: #{inspect(subscriptions)}" end)

    Events.subscriptions_set(subscriptions)
    |> to_server_sent_event()
    |> send_via(req)

    # Fetch current offsets before processing new subscriptions
    {client_id, req} = get_client_id(req)

    {:ok, offset_info} = Rig.Redis.get_client_offset_info(client_id)
    IO.inspect(offset_info, label: "SSE.set_subscriptions offset_info")

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

    IO.inspect(stored_offsets, label: "SSE.set_subscriptions stored_offsets")

    Enum.each(subscriptions, fn %Rig.Subscription{
                                  event_type: et,
                                  constraints: constraints,
                                  start_offset: offset
                                } ->
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
            IO.inspect({:replay_consumer, topic, et, partition, effective_offset}, label: "SSE.set_subscriptions starting replay")
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
        live_sub = %Rig.Subscription{
          event_type: et,
          constraints: constraints,
          start_offset: nil
        }

        EventFilter.refresh_subscriptions([live_sub], [])
      end
    end)

    new_state = Map.put(state, :subscriptions, subscriptions)
    {:ok, req, new_state, :hibernate}
  end

  @impl :cowboy_loop
  def info(:refresh_subscriptions, req, state) do
    EventFilter.refresh_subscriptions(state.subscriptions, [])
    Process.send_after(self(), :refresh_subscriptions, @subscription_refresh_interval_ms)
    {:ok, req, state, :hibernate}
  end

  @impl :cowboy_loop
  def info({:session_killed, session_id}, req, state) do
    Logger.info("Session killed: #{inspect(session_id)} - terminating SSE/#{inspect(self())}..")

    # We tell the client:
    :session_killed
    |> to_server_sent_event()
    |> send_via(req)

    # And close the connection:
    {:stop, req, state}
  end

  # ---

  @impl :cowboy_loop
  def terminate(reason, _req, state) do
    Logger.debug(fn ->
      pid = inspect(self())
      reason = "reason=" <> inspect(reason)
      "Closing SSE connection (#{pid}, #{reason})"
    end)

    if is_map(state) and Map.has_key?(state, :client_id) do
      Rig.Redis.delete_offsets(state.client_id)
    end

    :ok
  end

  # ---

  defp to_server_sent_event(:heartbeat), do: %{comment: "heartbeat"}

  defp to_server_sent_event(:session_killed) do
    %{
      specversion: "0.2",
      type: "rig.session_killed",
      source: "rig",
      id: UUID.uuid4(),
      time: Timex.now() |> Timex.format!("{RFC3339}")
    }
    |> CloudEvent.parse!()
    |> to_server_sent_event()
  end

  defp to_server_sent_event(%CloudEvent{} = event),
    do: %{
      data: event.json,
      event: CloudEvent.type!(event)
    }

  defp to_server_sent_event(event) when is_struct(event),
    do: %{
      data: Cloudevents.to_json(event),
      event: CloudEvent.type!(event)
    }

  # ---

  defp send_via(event, cowboy_req) do
    :cowboy_req.stream_events(event, :nofin, cowboy_req)
    Logger.debug(fn -> "Sent via SSE: " <> inspect(event) end)
  end

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
