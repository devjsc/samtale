# Samtale

A modern, minimal messaging framework for agents.

Samtale lets independent Python components exchange typed requests and
structured outcomes over HTTP. An agent can be a sensor, controller, service,
state machine, or LLM-backed application.

Samtale provides a uniform message envelope, Pydantic payload validation,
bounded handler concurrency, and three outcomes: `ok`, `rejected`, or `error`.
When an agent does not understand a request, it responds with the message types
and JSON schemas it accepts.

Samtale provides no broker, orchestration, memory, conversation history,
workflow engine, or LLM dependency.

```text
request → envelope validation → handler lookup → payload validation
        → handler execution → ok / rejected / error
```

Python 3.11 or later is required.

## Installation

```bash
pip install samtale
```

## A small agent

```python
from typing import Literal

from pydantic import BaseModel

from samtale import Agent, Message


class WeatherRequest(BaseModel):
    location: str
    unit: Literal["c", "f"] = "c"


weather = Agent("weather")


@weather.on("weather_request", model=WeatherRequest)
async def get_weather(message: Message, request: WeatherRequest):
    return {
        "location": request.location,
        "temperature": 18.4,
        "unit": request.unit,
    }


if __name__ == "__main__":
    weather.run(port=8003)
```

Call it from another agent:

```python
import asyncio

from samtale import Agent


async def main() -> None:
    consumer = Agent("consumer")

    try:
        result = await consumer.ask(
            "http://localhost:8003",
            "weather_request",
            location="London",
            unit="c",
        )
        print(result)
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
```

## Rejection is an outcome

An agent can understand a request and still decline it for a domain reason:

```python
from pydantic import BaseModel

from samtale import Message, Agent


class SetTemperature(BaseModel):
    value: float

weather = Agent("weather")

@weather.on("temperature.set", model=SetTemperature)
async def set_temperature(message: Message, request: SetTemperature):
    if request.value > 21:
        return weather.reject(
            message,
            "outside_supported_range",
            {
                "requested": request.value,
                "maximum": 21,
            },
        )

    return {"value": request.value}
```

`send()` returns that rejection normally, leaving the next decision to the
caller:

```python
from samtale import Status, Agent

consumer = Agent("consumer")

async def set_temperature_with_fallback() -> None:
    response = await consumer.send(
        "http://localhost:8003",
        "temperature.set",
        value=24,
    )

    if (
        response.status is Status.REJECTED
        and response.reason == "outside_supported_range"
    ):
        response = await consumer.send(
            "http://localhost:8003",
            "temperature.set",
            value=response.payload["maximum"],
        )
```

The framework standardises the exchange; the agents decide what happens next.

## Self-describing rejections

If the consumer sends an unsupported message type, the weather agent returns a
normal rejection with its accepted messages. The relevant response fields look
like this:

```json
{
  "status": "rejected",
  "reason": "unsupported_message",
  "payload": {
    "accepted_messages": [
      {
        "type": "weather_request",
        "schema": {
          "type": "object",
          "properties": {
            "location": {"type": "string"},
            "unit": {"enum": ["c", "f"], "default": "c"}
          },
          "required": ["location"]
        }
      }
    ]
  }
}
```

Invalid payloads are also rejected and include the expected message schema
alongside structured validation issues.

## Sending messages

The three outbound methods share the same acknowledged HTTP exchange:

- `send()` returns the complete response `Message`, including rejections.
- `ask()` expects success and returns only the response payload.
- `emit()` expects success and discards the response payload.

`ask()` and `emit()` raise `RemoteRejection` for ordinary domain rejections and
`RemoteError` for unexpected remote failures. `emit()` is a convenience method,
not guaranteed delivery or true fire-and-forget.

Handlers registered with `@agent.on(...)` must be defined with `async def`.
Synchronous handlers are rejected during registration so an invalid handler does
not fail only after receiving a request.

## Timeouts

`request_timeout` controls outbound HTTP operations made by `send()`, `ask()`,
and `emit()`. If the remote agent does not complete the exchange within the
HTTP client's timeout limits, the caller receives `SendTimeout`.

`handler_timeout` independently limits one local handler invocation. When the
limit is exceeded, the invocation is cancelled and the agent returns an `error`
message with reason `handler_timeout` using HTTP 504:

```python
# Calls other agents, with an outbound timeout
consumer = Agent("consumer", request_timeout=5.0)

# Cancels local handlers that run too long
weather = Agent("weather", handler_timeout=10.0)
```

These values are local limits. Samtale does not propagate a deadline through a
chain of agents, and time already spent by an upstream agent is not subtracted
from a downstream timeout. Applications that need an end-to-end deadline must
carry and enforce one as part of their own message protocol.

## Concurrency

Agents execute one handler at a time by default. Set `max_concurrency` when an
agent should continue processing while another handler awaits I/O:

```python
agent = Agent("weather", max_concurrency=8)
```

At most eight handlers execute simultaneously; additional messages remain in
the inbox. Concurrent handlers can access the same local state across `await`
points, so applications should protect shared mutable state when necessary.

The inbox is unlimited by default. Set `max_queue_size` to bound the number of
requests waiting for a handler:

```python
agent = Agent(
    "weather",
    max_concurrency=4,
    max_queue_size=20,
)
```

Here, four handlers may run while twenty additional requests wait. Once the
inbox is full, new requests are not queued. They receive an immediate
`rejected` response with reason `agent_busy` and payload `{"retryable": true}`.
The response uses HTTP 200; `send()` returns it normally, while `ask()` and
`emit()` raise `RemoteRejection`. Samtale does not retry automatically.

## Wire format

A request is a JSON object:

```json
{
  "id": "request-id",
  "sender": "consumer",
  "type": "weather_request",
  "payload": {"location": "London", "unit": "c"}
}
```

The response retains the domain type and correlates itself with `reply_to`:

```json
{
  "id": "response-id",
  "sender": "weather",
  "type": "weather_request",
  "payload": {"location": "London", "temperature": 18.4, "unit": "c"},
  "reply_to": "request-id",
  "status": "ok"
}
```

HTTP status describes the HTTP-level outcome. Message status describes the
domain outcome. Rejections use HTTP 200; malformed input and runtime failures
use the corresponding HTTP error status.

## Run the included example

From a checkout:

```bash
uv sync --locked
uv run python examples/weather.py
```

In another terminal:

```bash
uv run python examples/weather_consumer.py
```

The consumer first demonstrates schema discovery with an unsupported request,
then sends a valid typed request.

## Tests

```bash
uv run python -m unittest discover -s tests -v
```

The GitHub Actions workflow runs the suite on Python 3.11, 3.12, and 3.13.
