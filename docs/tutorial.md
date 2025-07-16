---
id: tutorial
title: Tutorial
sidebar_label: Tutorial
---

This tutorial shows a basic use case for RIG. A frontend (e.g. the mobile app for a chatroom service) connects to RIG and subscribes to a certain event type (e.g. messages from a chatroom). The backend (e.g. chatroom server) publishes the message to RIG, and RIG forwards it to the frontend.

We simulate frontend and backend HTTP requests using [HTTPie](https://httpie.org/) for HTTP requests, but of course you can also use curl or any other HTTP client. Please note that HTTPie sets the content type to `application/json` automatically, whereas for curl you need to use `-H "Content-Type: application/json"` for all but `GET` requests.

## 1. Start RIG

To get started, run our Docker image using this command:

```bash
$ docker run -p 4000:4000 -p 4010:4010 accenture/reactive-interaction-gateway
...
Reactive Interaction Gateway 2.1.0 [rig@127.0.0.1, ERTS 10.2.2, OTP 21]
```

Note that HTTPS is not enabled by default. Please read the [RIG operator guide](rig-ops-guide.md) before running a production setup.

## 2. Create a connection [Frontend]

Let's connect to RIG using [Server-Sent Events](https://en.wikipedia.org/wiki/Server-sent_events), which is our recommended approach (open standard, firewall friendly, plays nicely with HTTP/2):

```bash
$ http --stream :4000/_rig/v1/connection/sse
HTTP/1.1 200 OK
connection: keep-alive
content-type: text/event-stream
transfer-encoding: chunked
...

event: rig.connection.create
data: {"specversion":"0.2","source":"rig","type":"rig.connection.create","time":"2018-08-22T10:06:04.730484+00:00","id":"2b0a4f05-9032-4617-8d1e-92d97fb870dd","data":{"replay_token":"g2dkAA1ub25vZGVAbm9ob3N0AAACrAAAAAAA","errors":[]}}
id: 2b0a4f05-9032-4617-8d1e-92d97fb870dd
```

After the connection has been established, RIG sends out a [CloudEvent](https://github.com/cloudevents/spec/blob/v0.2/spec.md) of type `rig.connection.create`.

> You can see that ID and event type of the outer event (= SSE event) match ID and event type of the inner event (= CloudEvent). The cloud event is serialized to the `data` field.

Please take note of the `replay_token` in the CloudEvent's `data` field - you need it in the next step.

## 3. Subscribe to a topic [Frontend]

With the connection established, you can create _subscriptions_ - that is, you can tell RIG which events your app is interested in. RIG needs to know which connection you are referring to, so you need to use the connection token you have noted down in the last step:

```bash
$ CONN_TOKEN="g2dkAA1ub25vZGVAbm9ob3N0AAACrAAAAAAA"
$ SUBSCRIPTIONS='{"subscriptions":[{"eventType":"chatroom_message"}]}'
$ http put ":4000/_rig/v1/connection/sse/${CONN_TOKEN}/subscriptions" <<<"$SUBSCRIPTIONS"
HTTP/1.1 204 No Content
content-type: application/json; charset=utf-8
...
```

With that you're ready to receive all "chatroom_message" events.

## 4. Create a new "chatroom_message" event [Backend]

RIG expects to receive [CloudEvents](https://github.com/cloudevents/spec), so the following fields are required:

- `specversion`: must be set to "0.2" (version "0.1" is also supported).
- `type`: Type of occurrence which has happened. Often this attribute is used for routing, observability, policy enforcement, etc.
- `id`: ID of the event. The semantics of this string are explicitly undefined to ease the implementation of producers. Enables deduplication.
- `source`: This describes the event producer. Often this will include information such as the type of the event source, the organization publishing the event, the process that produced the event, and some unique identifiers. The exact syntax and semantics behind the data encoded in the URI is event producer defined.

Let's send a simple `chatroom_message` event:

```bash
$ http post :4000/_rig/v1/events \
  specversion=0.2 \
  type=chatroom_message \
  id=first-event \
  source=tutorial
HTTP/1.1 202 Accepted
content-type: application/json; charset=utf-8
...

{
    "specversion": "0.2",
    "id": "first-event",
    "time": "2018-08-21T09:11:27.614970+00:00",
    "type": "chatroom_message",
    "source": "tutorial"
}

```

RIG responds with `202 Accepted`, followed by the CloudEvent as sent to subscribers.

> If there are no subscribers for a received event, the response will still be `202 Accepted` and the event will be silently dropped.

## 5. The event has been delivered to our subscriber [Frontend]

Going back to the first terminal window you should now see your greeting event

# Connect your app to RIG

In a real-world frontend app the above example to connect your app to RIG would look something like this below.

See [**examples/sse-demo.html**](https://github.com/Accenture/reactive-interaction-gateway/blob/master/examples/sse-demo.html) for a full example.

## Basic Connection Example

```html
<!DOCTYPE html>
<html>
  <head>
    ...
    <script src="https://unpkg.com/event-source-polyfill/src/eventsource.min.js"></script>
  </head>

  <body>
    ...

    <script>
      ...

      const source = new EventSource(`http://localhost:4000/_rig/v1/connection/sse`)

      source.onopen = (e) => console.log("open", e)
      source.onmessage = (e) => console.log("message", e)
      source.onerror = (e) => console.log("error", e)

      source.addEventListener("rig.connection.create", function (e) {
        cloudEvent = JSON.parse(e.data)
        payload = cloudEvent.data
        connectionToken = payload["replay_token"]
        createSubscription(connectionToken)
      }, false);

      source.addEventListener("greeting", function (e) {
        cloudEvent = JSON.parse(e.data)
        ...
      })

      source.addEventListener("error", function (e) {
        if (e.readyState == EventSource.CLOSED) {
          console.log("Connection was closed.")
        } else {
          console.log("Connection error:", e)
        }
      }, false);

      function createSubscription(connectionToken) {
        const eventType = "greeting"
        return fetch(`http://localhost:4000/_rig/v1/connection/sse/${connectionToken}/subscriptions`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              "subscriptions": [{
                "eventType": eventType
              }]
            })
          })
          ...
      }
    </script>
  </body>
</html>
```

## Advanced Connection with Client ID Persistence (Optional)

For applications that need to handle reconnections and receive missed messages, you can optionally persist the client ID. This allows RIG to replay events that were sent while the client was disconnected.

```html
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>RIG SSE with Client ID Persistence</title>
    <script src="https://unpkg.com/event-source-polyfill/src/eventsource.min.js"></script>
  </head>

  <body>
    <h1>RIG SSE with Reconnection Support</h1>
    <div id="log"></div>

    <script>
      // Helper to get a cookie by name
      function getCookie(name) {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        if (parts.length === 2) return parts.pop().split(";").shift();
        return null;
      }

      // Helper to set a cookie
      function setCookie(name, value, options = {}) {
        const cookieOptions = {
          path: "/",
          "max-age": 31536000, // 1 year
          samesite: "Lax",
          ...options,
        };

        const cookieString = Object.entries(cookieOptions)
          .map(([key, val]) => `${key}=${val}`)
          .join("; ");

        document.cookie = `${name}=${value}; ${cookieString}`;
      }

      // Check for existing client ID
      const existingClientId = getCookie("replay_token");

      // Define your subscriptions
      const subscriptions = [
        {
          eventType: "chatroom_message",
          oneOf: [{}], // No constraints - receive all messages
          enable_replay: true, // <--- Enable replay for this event type
          cache_ttl: 60,       // <--- Store offsets for 60 seconds (for demo/testing)
        },
      ];

      const subscriptionsJson = JSON.stringify(subscriptions);
      const subscriptionsParam = encodeURIComponent(subscriptionsJson);

      // Build the SSE URL with optional client ID
      let eventSourceUrl = "http://localhost:4000/_rig/v1/connection/sse?";
      if (existingClientId) {
        eventSourceUrl += `replay_token=${existingClientId}&`;
      }
      eventSourceUrl += `subscriptions=${subscriptionsParam}`;

      console.log(`Connecting to SSE at: ${eventSourceUrl}`);

      // Create the EventSource connection
      const eventSource = new EventSource(eventSourceUrl);

      // Connection opened
      eventSource.addEventListener("open", () => {
        console.log("[SSE] Connection opened");
        logMessage("[SSE] Connection opened");
      });

      // Handle connection creation and store client ID
      eventSource.addEventListener("rig.connection.create", (evt) => {
        console.log(`[rig.connection.create] ${evt.data}`);
        const parsed = JSON.parse(evt.data);
        const clientId = parsed.data.client_id;

        if (clientId) {
          // Store the client ID for future reconnections
          setCookie("replay_token", clientId);
          console.log(`[rig.connection.create] Client ID stored: ${clientId}`);
          logMessage(`[rig.connection.create] Client ID stored: ${clientId}`);
        }
      });

      // Handle subscription confirmation
      eventSource.addEventListener("rig.subscriptions_set", (evt) => {
        console.log(`[rig.subscriptions_set] ${evt.data}`);
        logMessage(`[rig.subscriptions_set] ${evt.data}`);
      });

      // Handle chatroom messages
      eventSource.addEventListener("chatroom_message", (evt) => {
        console.log(`[chatroom_message] Raw data: ${evt.data}`);
        try {
          const parsed = JSON.parse(evt.data);
          console.log(`[chatroom_message] Parsed:`, parsed);
          logMessage(`[chatroom_message] ${JSON.stringify(parsed, null, 2)}`);
        } catch (err) {
          console.error(`[chatroom_message] JSON.parse error: ${err}`);
          logMessage(`[chatroom_message] JSON.parse error: ${err}`);
        }
      });

      // Handle offset errors (e.g., when requested offset is out of range)
      eventSource.addEventListener("rig.offset_error", (evt) => {
        console.error(`[rig.offset_error] ${evt.data}`);
        logMessage(`[rig.offset_error] ${evt.data}`);
      });

      // Handle heartbeats and other messages
      eventSource.onmessage = (evt) => {
        if (evt.data.trim() === "") {
          console.log(`[heartbeat] (empty frame)`);
          logMessage(`[heartbeat] (empty frame)`);
        } else {
          console.log(`[message] ${evt.data}`);
          logMessage(`[message] ${evt.data}`);
        }
      };

      // Handle connection errors
      eventSource.onerror = (err) => {
        console.error(`[SSE error] ${err}`);
        logMessage(`[SSE error] ${err}`);
      };

      // Helper function to log messages to the UI
      function logMessage(msg) {
        const logEl = document.getElementById("log");
        const line = document.createElement("div");
        line.textContent = new Date().toISOString() + " " + msg;
        logEl.appendChild(line);
        logEl.scrollTop = logEl.scrollHeight;
      }
    </script>
  </body>
</html>
```

### Key Features of the Advanced Example:

1. **Client ID Persistence**: The `replay_token` is stored in a cookie and reused on reconnection
2. **Automatic Replay**: When reconnecting with a stored client ID, RIG will replay any messages that were sent while the client was disconnected
3. **Subscription in URL**: Subscriptions are passed as URL parameters, eliminating the need for a separate subscription request
4. **Error Handling**: Proper handling of offset errors and connection issues
5. **Heartbeat Support**: Handles empty frames used for connection keep-alive

### When to Use Client ID Persistence:

- **Use it when**: You need to ensure no messages are lost during temporary disconnections
- **Don't use it when**: You only want to receive live messages and don't need historical replay
- **Consider**: Client ID persistence requires server-side storage and may have performance implications for long-running connections

The basic example is sufficient for most use cases, while the advanced example provides additional reliability for applications that cannot afford to miss messages.
