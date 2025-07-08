defmodule RigKafka.ReplayKafkaConsumer do
  @moduledoc """
  One‐shot Kafka fetch from `start_offset` → head, filtering by `event_type` and `constraints`,
  forwarding each matched CloudEvent to `conn_pid`, then registering that pid for live ETS‐filtered dispatch.
  """

  use GenServer
  require Logger

  alias Rig.EventFilter
  alias Rig.Subscription
  alias RigCloudEvents.CloudEvent
  alias UUID

  @default_topic Application.get_env(:rig, :kafka_topic, "rig")

  @type t :: %{
          conn_pid: pid(),
          event_type: String.t(),
          constraints: [%{String.t() => String.t()}],
          current_offset: non_neg_integer(),
          partition: non_neg_integer()
        }

  # ──────────────────────────────────────────────────────────────────────────────
  # Public API – start a one‐shot replay for this connection + subscription
  # ──────────────────────────────────────────────────────────────────────────────
  def start_link(%{
        conn_pid: conn_pid,
        event_type: event_type,
        constraints: constraints,
        start_offset: start_offset,
        partition: partition
      }) do
    initial_state = %{
      conn_pid: conn_pid,
      event_type: event_type,
      constraints: constraints,
      current_offset: start_offset,
      partition: partition
    }

    GenServer.start_link(__MODULE__, initial_state)
  end

  # ──────────────────────────────────────────────────────────────────────────────
  @impl true
  def init(%{conn_pid: _conn_pid} = state) do
    # Kick off replay loop immediately
    send(self(), :begin_replay)
    {:ok, state}
  end

  # ──────────────────────────────────────────────────────────────────────────────
  @impl true
  def handle_info(
        :begin_replay,
        %{
          conn_pid: conn_pid,
          event_type: event_type,
          constraints: constraints,
          current_offset: offset,
          partition: partition
        } = state
      ) do
    # ──────────────────────────────────────────────────────────────────────────────
    # 1) Read Kafka‐broker configuration exactly as KafkaToFilter does
    # ──────────────────────────────────────────────────────────────────────────────
    kafka_conf = Confex.get_env(:rig, Rig.EventStream.KafkaToFilter, [])
    brokers_strings = Keyword.get(kafka_conf, :brokers, ["localhost:9092"])

    # Convert ["host:port"] → [{"host", port}]
    brokers =
      Enum.map(brokers_strings, fn broker_string ->
        [host, port_str] = String.split(broker_string, ":", parts: 2)
        {host, String.to_integer(port_str)}
      end)

    Logger.debug("Using Kafka brokers: #{inspect(brokers)}")

    # ──────────────────────────────────────────────────────────────────────────────
    # 2) Build a unique client‐ID atom for :brod
    # ──────────────────────────────────────────────────────────────────────────────
    client_id = :"replay_consumer_#{inspect(conn_pid)}_#{System.unique_integer([:positive])}"
    Logger.debug("Starting Kafka client with ID: #{client_id}")

    # ──────────────────────────────────────────────────────────────────────────────
    # 3) Start the Brod client (we keep the PID around only for stopping later)
    # ──────────────────────────────────────────────────────────────────────────────
    case :brod.start_link_client(brokers, client_id, []) do
      {:ok, client_pid} ->
        Logger.debug("Successfully started Kafka client PID: #{inspect(client_pid)}")

        # ────────────────────────────────────────────────────────────────────────────
        # 4) Look up the head (latest) offset for this topic/partition
        # ────────────────────────────────────────────────────────────────────────────
        Logger.debug("Resolving latest offset for topic #{@default_topic}")

        case :brod.resolve_offset(brokers, @default_topic, partition, :latest) do
          {:ok, head_offset} ->
            Logger.debug("Successfully resolved head offset: #{head_offset}")

            # ────────────────────────────────────────────────────────────────────────
            # 5) Replay loop from requested `offset` → `head_offset`.
            #    We pass `client_id` (an atom) into fetch itself.
            # ────────────────────────────────────────────────────────────────────────
            do_replay_loop(
              # << pass the client‐ID atom, not the PID >>
              client_id,
              @default_topic,
              partition,
              offset,
              head_offset,
              event_type,
              constraints,
              conn_pid
            )

            # ────────────────────────────────────────────────────────────────────────
            # 6) Once replay is done, register for live events (start_offset: nil)
            # ────────────────────────────────────────────────────────────────────────
            live_sub = %Subscription{
              event_type: event_type,
              constraints: constraints,
              start_offset: nil
            }

            Logger.debug("Registering for live events with subscription: #{inspect(live_sub)}")
            EventFilter.refresh_subscriptions([live_sub], [])

            # ────────────────────────────────────────────────────────────────────────
            # 7) Stop the Brod client, then terminate this GenServer
            # ────────────────────────────────────────────────────────────────────────
            Logger.debug("Stopping Kafka client and GenServer")
            :brod.stop_client(client_pid)
            {:stop, :normal, state}

          {:error, {:offset_out_of_range, _}} ->
            Logger.warn("Offset #{offset} is out of range")

            # Send back a rig.offset_error CloudEvent
            error_event =
              %{
                specversion: "0.2",
                type: "rig.offset_error",
                source: "rig",
                id: UUID.uuid4(),
                time: DateTime.utc_now() |> DateTime.to_iso8601(),
                data: %{"error" => "offset_out_of_range", "requested_offset" => offset}
              }
              |> CloudEvent.parse!()

            Logger.debug("Sending offset error event: #{inspect(error_event)}")
            send(conn_pid, error_event)

            # Still register for live‐only
            live_sub = %Subscription{
              event_type: event_type,
              constraints: constraints,
              start_offset: nil
            }

            Logger.debug("Registering for live events only: #{inspect(live_sub)}")
            EventFilter.refresh_subscriptions([live_sub], [])

            :brod.stop_client(client_pid)
            {:stop, :normal, state}

          {:error, reason} ->
            Logger.warn("Could not resolve latest offset: #{inspect(reason)}")

            error_event =
              %{
                specversion: "0.2",
                type: "rig.offset_error",
                source: "rig",
                id: UUID.uuid4(),
                time: DateTime.utc_now() |> DateTime.to_iso8601(),
                data: %{"error" => "timeout", "requested_offset" => offset}
              }
              |> CloudEvent.parse!()

            Logger.debug("Sending timeout error event: #{inspect(error_event)}")
            send(conn_pid, error_event)

            :brod.stop_client(client_pid)
            {:stop, :normal, state}
        end

      {:error, reason} ->
        Logger.error("Failed to start Kafka client: #{inspect(reason)}")
        {:stop, :normal, state}
    end
  end

  # ──────────────────────────────────────────────────────────────────────────────
  # do_replay_loop/8:
  #   - Fetch in batches, decode CloudEvent JSON, filter by type & constraints
  #   - Each message is now a {:kafka_message, offset, key, value, type, ts, headers} tuple
  #   - Forward every matched event to conn_pid with added kafka_offset/partition/topic
  # ──────────────────────────────────────────────────────────────────────────────
  defp do_replay_loop(
         client_id,
         topic,
         partition,
         current_offset,
         head_offset,
         event_type,
         constraints,
         conn_pid
       )
       when current_offset <= head_offset do
    # Re‐derive the same brokers list for logging (optional)
    kafka_conf = Confex.get_env(:rig, Rig.EventStream.KafkaToFilter, [])
    brokers_strings = Keyword.get(kafka_conf, :brokers, ["localhost:9092"])

    brokers =
      Enum.map(brokers_strings, fn broker_string ->
        [host, port_str] = String.split(broker_string, ":", parts: 2)
        {host, String.to_integer(port_str)}
      end)

    fetch_opts = %{max_bytes: 100 * 1024, timeout: 5_000}

    Logger.debug("Fetching messages from offset #{current_offset} to #{head_offset}")
    Logger.debug("Using Brod client ID: #{inspect(client_id)}")
    Logger.debug("Bootstrap list: #{inspect(brokers)}, Topic: #{topic}, Partition: #{partition}")

    try do
      case :brod.fetch(client_id, topic, partition, current_offset, fetch_opts) do
        {:ok, {hw_offset, messages}} ->
          Logger.debug(
            "Received #{length(messages)} messages from Kafka (head‐offset #{hw_offset})"
          )

          if messages == [] do
            Logger.debug("No messages returned at offset #{current_offset}")
          end

          for message <- messages do
            case message do
              # Brod 3.9 returns {:kafka_message, offset, key, value, type, ts, headers}
              {:kafka_message, o, _key, raw_payload, _type, _ts, _headers} ->
                Logger.debug("Processing message at offset #{o}: #{inspect(raw_payload)}")

                # === THIS is where we changed the pattern to match on the struct ===
                case Cloudevents.from_kafka_message(raw_payload, []) do
                  {:ok,
                   %Cloudevents.Format.V_0_2.Event{
                     type: ^event_type,
                     data: data
                   } = cloud_event} ->
                    Logger.debug("Found matching event type at offset #{o}")

                    if matches_constraints?(data, constraints) do
                      Logger.debug("Matched event at offset #{o}: #{inspect(data)}")

                      enriched =
                        cloud_event
                        |> Map.put("topic", topic)
                        |> Map.put(
                          :extensions,
                          Map.merge(cloud_event.extensions, %{
                            "x-kafka-offset" => o,
                            "x-kafka-partition" => partition
                          })
                        )

                      Logger.debug(
                        "Sending enriched event to conn_pid #{inspect(conn_pid)}: #{
                          inspect(enriched)
                        }"
                      )

                      send(conn_pid, enriched)
                    else
                      Logger.debug(
                        "Event at offset #{o} did not match constraints: #{inspect(data)}"
                      )
                    end

                  {:ok, %Cloudevents.Format.V_0_2.Event{} = other_event} ->
                    Logger.debug("Skipping non‐matching event type: #{inspect(other_event)}")

                  {:error, reason} ->
                    Logger.warn("Failed to decode message at offset #{o}: #{inspect(reason)}")

                  err ->
                    Logger.warn(
                      "Cloudevents.from_kafka_message returned unexpected: #{inspect(err)}"
                    )
                end

              other ->
                Logger.warn("Unexpected message format returned by Brod.fetch: #{inspect(other)}")
            end
          end

          # Move to next_offset = (last_message_offset + 1), or jump past head if none
          new_offset =
            case List.last(messages) do
              {:kafka_message, last_o, _key, _val, _type, _ts, _hdrs} -> last_o + 1
              _ -> head_offset + 1
            end

          Logger.debug("Moving to next offset: #{new_offset}")

          if new_offset <= head_offset do
            do_replay_loop(
              client_id,
              topic,
              partition,
              new_offset,
              head_offset,
              event_type,
              constraints,
              conn_pid
            )
          else
            Logger.debug("Reached head offset #{head_offset}, stopping replay")
            :ok
          end

        {:error, reason} ->
          Logger.warn("Replay fetch error at offset #{current_offset}: #{inspect(reason)}")
          :ok
      end
    rescue
      e ->
        Logger.error("Error in do_replay_loop: #{inspect(e)}")
        Logger.error("Stacktrace: #{Exception.format_stacktrace(__STACKTRACE__)}")
        :ok
    end
  end

  # If `current_offset > head_offset`, we're done
  defp do_replay_loop(_, _, _, _, _, _, _, _), do: :ok

  # ──────────────────────────────────────────────────────────────────────────────
  # Constraint‐matching helper (unchanged)
  # ──────────────────────────────────────────────────────────────────────────────
  defp matches_constraints?(data, constraints) do
    result =
      Enum.any?(constraints, fn clause ->
        matches =
          Enum.all?(clause, fn {k, v} ->
            actual = Map.get(data, k)
            Logger.debug("Checking constraint #{k}=#{v} against actual value #{inspect(actual)}")
            actual == v
          end)

        Logger.debug("Clause #{inspect(clause)} matches: #{matches}")
        matches
      end)

    Logger.debug("Final constraint match result: #{result}")
    result
  end
end
