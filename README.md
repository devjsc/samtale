# chatters

`chatters` is a modern, minimal messaging framework for agents.

A minimal library for building HTTP-based agents that communicate with each other using a simple JSON message envelope. Agents can register handlers for specific message types, validate payloads using Pydantic models, and manage concurrency for handling multiple messages simultaneously. 



```python
from chatters import Agent, Message
from pydantic import BaseModel

temperature = Agent("temperature", max_concurrency=4)

class ReadTemperature(BaseModel):
    unit: str = "c"

@temperature.on("read", model=ReadTemperature)
async def read(message: Message, request: ReadTemperature):
    return {"value": 18.4, "unit": request.unit}

temperature.run(port=8001)
```

```python
from chatters import Agent

controller = Agent("controller")
try:
    reading = await controller.ask("http://localhost:8001", "read")
    await controller.emit(
        "http://localhost:8002",
        "temperature.changed",
        value=reading["value"],
    )
finally:
    await controller.close()
```

`send` waits for a complete response message. `ask` is the convenience form that expects success and returns only the payload. `emit` uses the same HTTP acknowledgement path, then discards the returned message; it is not guaranteed delivery or true fire and forget.

`ask` and `emit` raise `RemoteRejection` for ordinary remote rejections and
`RemoteError` for unexpected remote failures. `send` returns rejection
responses normally.

Agents execute one handler at a time by default. Set `max_concurrency` to allow
multiple handlers to run while others are awaiting I/O:

```python
weather = Agent("weather", max_concurrency=8)
```

At most eight handlers execute simultaneously; additional messages remain
queued. Concurrent handlers may access agent-local state across `await` points,
so applications are responsible for locking shared mutable state when needed.
The default remains `1` for straightforward sequential behavior.

## Wire envelope

Requests and responses are JSON objects:

```json
{
  "id": "uuid",
  "sender": "controller",
  "type": "read",
  "payload": {}
}
```

Replies are messages too. They keep the original domain `type`, include the original message ID as `reply_to`, and use `status` plus optional `reason` to describe the outcome.

Registering a handler with a Pydantic `model=` validates its payload and passes
the parsed model as the handler's second argument. Rejections for invalid or
unsupported messages include the expected JSON Schema so callers can discover
the formats the agent accepts. Handlers without `model=` retain the original
single-argument API.

## Tiny demo

Start a temperature agent in one terminal:

```bash
.venv/bin/python examples/temperature.py
```

Ask it for a reading from another terminal:

```bash
.venv/bin/python examples/controller.py
```

## Canonical example

Start the weather agent, which accepts a Pydantic `WeatherRequest` payload:

```bash
.venv/bin/python examples/weather.py
```

Then run the consumer:

```bash
.venv/bin/python examples/weather_consumer.py
```

The consumer first sends an unsupported message and prints the `rejected`
response, including the `weather_request` JSON Schema. It then uses that
contract to send a valid `weather_request` and prints the successful result.

## Tests
Written by an llm, for now.

```bash
.venv/bin/python -m unittest discover -s tests
```
