import asyncio
import json
import unittest
from unittest.mock import patch

import httpx
from pydantic import BaseModel

from samtale import (
    Agent,
    Message,
    MessageValidationError,
    RemoteError,
    RemoteRejection,
    Status,
    TransportError,
)


class AgentTests(unittest.TestCase):
    def test_agent_validates_and_normalizes_construction(self):
        with self.assertRaises(ValueError):
            Agent("")
        with self.assertRaises(ValueError):
            Agent("   ")
        with self.assertRaises(ValueError):
            Agent("agent", timeout=0)
        with self.assertRaises(ValueError):
            Agent("agent", timeout=float("nan"))
        with self.assertRaises(ValueError):
            Agent("agent", handler_timeout=-1)
        with self.assertRaises(ValueError):
            Agent("agent", max_concurrency=0)
        with self.assertRaises(ValueError):
            Agent("agent", max_concurrency=True)

        self.assertEqual(Agent("  weather  ").name, "weather")
        self.assertEqual(Agent("weather").max_concurrency, 1)

    def test_endpoint_dispatches_dict_result_as_ok_response(self):
        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.read")
            async def read(message):
                return {"value": 18.4, "unit": message.payload.get("unit", "c")}

            await agent.start()
            try:
                body = Message(sender="controller", type="temperature.read").to_dict()
                response = await call_app(agent.app, "/messages", body)
            finally:
                await agent.close()

            return body, response

        request_body, response = asyncio.run(main())
        self.assertEqual(response["status"], 200)
        self.assertEqual(response["json"]["type"], "temperature.read")
        self.assertEqual(response["json"]["reply_to"], request_body["id"])
        self.assertEqual(response["json"]["status"], "ok")
        self.assertEqual(response["json"]["payload"], {"value": 18.4, "unit": "c"})

    def test_none_handler_result_becomes_empty_ok_response(self):
        async def main():
            agent = Agent("sink")

            @agent.on("event.recorded")
            async def recorded(message):
                return None

            await agent.start()
            try:
                return await call_app(agent.app, "/messages", Message(sender="controller", type="event.recorded").to_dict())
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 200)
        self.assertEqual(response["json"]["status"], "ok")
        self.assertEqual(response["json"]["payload"], {})

    def test_handler_can_return_explicit_domain_rejection(self):
        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.set")
            async def set_temperature(message):
                return agent.reject(message, "outside_supported_range", {"maximum": 22})

            await agent.start()
            try:
                return await call_app(agent.app, "/messages", Message(sender="controller", type="temperature.set", payload={"value": 24}).to_dict())
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 200)
        self.assertEqual(response["json"]["status"], "rejected")
        self.assertEqual(response["json"]["reason"], "outside_supported_range")
        self.assertEqual(response["json"]["payload"], {"maximum": 22})

    def test_endpoint_rejects_malformed_message(self):
        async def main():
            agent = Agent("temperature")
            await agent.start()
            try:
                return await call_app(agent.app, "/messages", {"id": "1", "sender": "controller"})
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 400)
        self.assertEqual(response["json"]["type"], "_agent.error")
        self.assertEqual(response["json"]["status"], "error")
        self.assertEqual(response["json"]["reason"], "malformed_message")

    def test_endpoint_rejects_invalid_json_as_malformed_message(self):
        async def main():
            agent = Agent("temperature")
            await agent.start()
            try:
                return await call_app_raw(agent.app, "/messages", b"{not-json")
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 400)
        self.assertEqual(response["json"]["reason"], "malformed_message")

    def test_endpoint_rejects_response_envelope(self):
        async def main():
            agent = Agent("temperature")
            request = Message(sender="controller", type="temperature.read")
            response_message = Message.ok("other", request)
            await agent.start()
            try:
                return await call_app(agent.app, "/messages", response_message.to_dict())
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 400)
        self.assertEqual(response["json"]["reason"], "malformed_message")

    def test_closed_agent_rejects_new_requests_without_enqueuing(self):
        async def main():
            agent = Agent("temperature")
            await agent.start()
            await agent.close()
            response = await call_app(
                agent.app,
                "/messages",
                Message(sender="controller", type="temperature.read").to_dict(),
            )
            return response, agent.inbox.qsize()

        response, queue_size = asyncio.run(main())
        self.assertEqual(response["status"], 503)
        self.assertEqual(response["json"]["reason"], "agent_closed")
        self.assertEqual(queue_size, 0)

    def test_unknown_type_returns_sorted_unsupported_rejection(self):
        class SetTemperature(BaseModel):
            value: float

        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.set", model=SetTemperature)
            async def set_temperature(message, payload):
                return {}

            @agent.on("temperature.read")
            async def read(message):
                return {}

            await agent.start()
            try:
                return await call_app(agent.app, "/messages", Message(sender="controller", type="temperature.calibrate").to_dict())
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 200)
        self.assertEqual(response["json"]["status"], "rejected")
        self.assertEqual(response["json"]["reason"], "unsupported_message")
        accepted = response["json"]["payload"]["accepted_messages"]
        self.assertEqual([item["type"] for item in accepted], ["temperature.read", "temperature.set"])
        self.assertEqual(accepted[0]["schema"], {})
        self.assertEqual(accepted[1]["schema"]["required"], ["value"])
        self.assertEqual(accepted[1]["schema"]["properties"]["value"]["type"], "number")

    def test_pydantic_model_parses_payload_for_handler(self):
        class SetTemperature(BaseModel):
            value: float

        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.set", model=SetTemperature)
            async def set_temperature(message, payload):
                self.assertIsInstance(payload, SetTemperature)
                self.assertEqual(message.payload, {"value": 18})
                return {"value": payload.value}

            await agent.start()
            try:
                request = Message(sender="controller", type="temperature.set", payload={"value": 18})
                return await call_app(agent.app, "/messages", request.to_dict())
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["json"]["status"], "ok")
        self.assertEqual(response["json"]["payload"], {"value": 18.0})

    def test_pydantic_invalid_payload_returns_schema_and_issues(self):
        class SetTemperature(BaseModel):
            value: float

        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.set", model=SetTemperature)
            async def set_temperature(message, payload):
                return {"value": payload.value}

            await agent.start()
            try:
                request = Message(sender="controller", type="temperature.set", payload={"value": "hot"})
                return await call_app(agent.app, "/messages", request.to_dict())
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["json"]["status"], "rejected")
        self.assertEqual(response["json"]["reason"], "invalid_payload")
        accepted = response["json"]["payload"]["expected_message"]
        self.assertEqual(accepted["type"], "temperature.set")
        self.assertEqual(accepted["schema"]["properties"]["value"]["type"], "number")
        self.assertEqual(response["json"]["payload"]["issues"][0]["field"], "value")
        self.assertEqual(response["json"]["payload"]["issues"][0]["reason"], "float_parsing")

    def test_model_must_be_a_pydantic_model_class(self):
        agent = Agent("temperature")
        with self.assertRaises(TypeError):
            agent.on("temperature.set", model=dict)

    def test_handler_message_type_must_be_non_empty_text(self):
        agent = Agent("temperature")
        with self.assertRaises(ValueError):
            agent.on("")
        with self.assertRaises(ValueError):
            agent.on(None)

    def test_duplicate_handler_registration_is_rejected(self):
        agent = Agent("temperature")

        @agent.on("temperature.read")
        async def first(message):
            return {}

        with self.assertRaises(ValueError):
            agent.on("temperature.read")(first)

    def test_handler_can_return_explicit_ok_response(self):
        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.read")
            async def read(message):
                return agent.ok(message, {"value": 18.4})

            await agent.start()
            try:
                return await call_app(
                    agent.app,
                    "/messages",
                    Message(sender="controller", type="temperature.read").to_dict(),
                )
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["json"]["status"], "ok")
        self.assertEqual(response["json"]["payload"], {"value": 18.4})

    def test_handler_response_with_wrong_correlation_is_invalid(self):
        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.read")
            async def read(message):
                other = Message(sender="controller", type="temperature.read")
                return Message.ok("temperature", other, {"value": 18.4})

            await agent.start()
            try:
                return await call_app(
                    agent.app,
                    "/messages",
                    Message(sender="controller", type="temperature.read").to_dict(),
                )
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 500)
        self.assertEqual(response["json"]["reason"], "invalid_handler_result")

    def test_handler_response_sender_is_normalized(self):
        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.read")
            async def read(message):
                return Message.ok("wrong-sender", message, {"value": 18.4})

            await agent.start()
            try:
                return await call_app(
                    agent.app,
                    "/messages",
                    Message(sender="controller", type="temperature.read").to_dict(),
                )
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["json"]["status"], "ok")
        self.assertEqual(response["json"]["sender"], "temperature")

    def test_non_json_handler_result_returns_structured_error(self):
        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.read")
            async def read(message):
                return {"value": object()}

            await agent.start()
            try:
                return await call_app(
                    agent.app,
                    "/messages",
                    Message(sender="controller", type="temperature.read").to_dict(),
                )
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 500)
        self.assertEqual(response["json"]["reason"], "invalid_handler_result")

    def test_endpoint_returns_handler_failure_without_detail_leakage(self):
        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.read")
            async def read(message):
                raise RuntimeError("sensor offline at /secret/path")

            await agent.start()
            try:
                return await call_app(agent.app, "/messages", Message(sender="controller", type="temperature.read").to_dict())
            finally:
                await agent.close()

        with self.assertLogs("samtale.helpers", level="ERROR") as logs:
            response = asyncio.run(main())
        self.assertEqual(response["status"], 500)
        self.assertEqual(response["json"]["status"], "error")
        self.assertEqual(response["json"]["reason"], "handler_failure")
        self.assertEqual(response["json"]["payload"]["message"], "The message handler failed")
        self.assertNotIn("sensor offline", response["json"]["payload"]["message"])
        self.assertIn("Handler 'temperature.read' failed", logs.output[0])

    def test_endpoint_returns_handler_timeout(self):
        async def main():
            agent = Agent("temperature", handler_timeout=0.001)

            @agent.on("temperature.read")
            async def read(message):
                await asyncio.sleep(1)
                return {"value": 18.4}

            await agent.start()
            try:
                return await call_app(agent.app, "/messages", Message(sender="controller", type="temperature.read").to_dict())
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 504)
        self.assertEqual(response["json"]["status"], "error")
        self.assertEqual(response["json"]["reason"], "handler_timeout")

    def test_send_posts_message_and_returns_full_response(self):
        agent = Agent("controller")

        class Response:
            status_code = 200

            def __init__(self, data):
                self._data = data
                self.request = httpx.Request("POST", "http://example.test/messages")

            def raise_for_status(self):
                pass

            def json(self):
                return self._data

        class Client:
            last = None

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.is_closed = False
                Client.last = self

            async def post(self, url, json):
                self.url = url
                self.json = json
                return Response(Message.ok("temperature", Message.from_dict(json), {"value": 18.4}).to_dict())

            async def aclose(self):
                self.is_closed = True

        async def main():
            with patch("samtale.agent.httpx.AsyncClient", Client):
                try:
                    response = await agent.send("http://example.test", "temperature.read")
                    self.assertIs(Client.last, agent._client)
                    return response
                finally:
                    await agent.close()

        response = asyncio.run(main())
        self.assertIs(response.status, Status.OK)
        self.assertEqual(response.payload, {"value": 18.4})

    def test_ask_returns_payload_for_ok_response(self):
        agent = Agent("controller")

        class Response:
            status_code = 200

            def __init__(self, data):
                self._data = data

            def raise_for_status(self):
                pass

            def json(self):
                return self._data

        class Client:
            def __init__(self, **kwargs):
                self.is_closed = False

            async def post(self, url, json):
                return Response(Message.ok("temperature", Message.from_dict(json), {"value": 18.4}).to_dict())

            async def aclose(self):
                self.is_closed = True

        async def main():
            with patch("samtale.agent.httpx.AsyncClient", Client):
                try:
                    return await agent.ask("http://example.test", "temperature.read")
                finally:
                    await agent.close()

        self.assertEqual(asyncio.run(main()), {"value": 18.4})

    def test_send_returns_rejection_without_transport_exception(self):
        agent = Agent("controller")

        class Response:
            status_code = 200

            def __init__(self, data):
                self._data = data

            def raise_for_status(self):
                pass

            def json(self):
                return self._data

        class Client:
            def __init__(self, **kwargs):
                self.is_closed = False

            async def post(self, url, json):
                return Response(Message.reject("temperature", Message.from_dict(json), "outside_supported_range", {"maximum": 22}).to_dict())

            async def aclose(self):
                self.is_closed = True

        async def main():
            with patch("samtale.agent.httpx.AsyncClient", Client):
                try:
                    return await agent.send("http://example.test", "temperature.set", value=24)
                finally:
                    await agent.close()

        response = asyncio.run(main())
        self.assertIs(response.status, Status.REJECTED)
        self.assertEqual(response.reason, "outside_supported_range")

    def test_ask_raises_remote_rejection_for_rejection(self):
        agent = Agent("controller")

        class Response:
            status_code = 200

            def __init__(self, data):
                self._data = data

            def raise_for_status(self):
                pass

            def json(self):
                return self._data

        class Client:
            def __init__(self, **kwargs):
                self.is_closed = False

            async def post(self, url, json):
                return Response(Message.reject("temperature", Message.from_dict(json), "outside_supported_range", {"maximum": 22}).to_dict())

            async def aclose(self):
                self.is_closed = True

        async def main():
            with patch("samtale.agent.httpx.AsyncClient", Client):
                try:
                    with self.assertRaises(RemoteRejection) as raised:
                        await agent.ask("http://example.test", "temperature.set", value=24)
                    self.assertEqual(raised.exception.reason, "outside_supported_range")
                    self.assertEqual(raised.exception.payload, {"maximum": 22})
                finally:
                    await agent.close()

        asyncio.run(main())

    def test_send_rejects_non_json_payload_before_transport(self):
        agent = Agent("controller")

        async def main():
            with self.assertRaises(MessageValidationError):
                await agent.send("http://example.test", "temperature.set", value=object())

        asyncio.run(main())
        self.assertIsNone(agent._client)

    def test_send_rejects_mismatched_reply(self):
        agent = Agent("controller")

        class Response:
            status_code = 200

            def raise_for_status(self):
                pass

            def json(self):
                request = Message(sender="controller", type="temperature.read")
                return Message.ok("temperature", request, {}).to_dict()

        class Client:
            def __init__(self, **kwargs):
                self.is_closed = False

            async def post(self, url, json):
                return Response()

            async def aclose(self):
                self.is_closed = True

        async def main():
            with patch("samtale.agent.httpx.AsyncClient", Client):
                try:
                    with self.assertRaises(TransportError):
                        await agent.send("http://example.test", "temperature.read")
                finally:
                    await agent.close()

        asyncio.run(main())

    def test_send_rejects_mismatched_type(self):
        agent = Agent("controller")

        class Response:
            status_code = 200

            def __init__(self, data):
                self._data = data

            def raise_for_status(self):
                pass

            def json(self):
                return self._data

        class Client:
            def __init__(self, **kwargs):
                self.is_closed = False

            async def post(self, url, json):
                request = Message.from_dict(json)
                return Response(Message(sender="temperature", type="other", payload={}, reply_to=request.id, status=Status.OK).to_dict())

            async def aclose(self):
                self.is_closed = True

        async def main():
            with patch("samtale.agent.httpx.AsyncClient", Client):
                try:
                    with self.assertRaises(TransportError):
                        await agent.send("http://example.test", "temperature.read")
                finally:
                    await agent.close()

        asyncio.run(main())

    def test_send_translates_http_error_envelope(self):
        agent = Agent("controller")

        class Response:
            status_code = 500

            def __init__(self, data):
                self._data = data
                self.request = httpx.Request("POST", "http://example.test/messages")

            def raise_for_status(self):
                raise httpx.HTTPStatusError("server error", request=self.request, response=self)

            def json(self):
                return self._data

        class Client:
            def __init__(self, **kwargs):
                self.is_closed = False

            async def post(self, url, json):
                return Response(Message.error("temperature", Message.from_dict(json), "handler_failure", {"message": "The message handler failed"}).to_dict())

            async def aclose(self):
                self.is_closed = True

        async def main():
            with patch("samtale.agent.httpx.AsyncClient", Client):
                try:
                    with self.assertRaises(RemoteError) as raised:
                        await agent.send("http://example.test", "temperature.read")
                    self.assertEqual(raised.exception.error["reason"], "handler_failure")
                finally:
                    await agent.close()

        asyncio.run(main())

    def test_send_treats_uncorrelated_framework_error_as_transport_error(self):
        agent = Agent("controller")

        class Response:
            status_code = 400

            def __init__(self):
                self.request = httpx.Request("POST", "http://example.test/messages")

            def raise_for_status(self):
                raise httpx.HTTPStatusError(
                    "bad request", request=self.request, response=self
                )

            def json(self):
                return Message.error(
                    "temperature",
                    None,
                    "malformed_message",
                    {"message": "invalid envelope"},
                ).to_dict()

        class Client:
            def __init__(self, **kwargs):
                self.is_closed = False

            async def post(self, url, json):
                return Response()

            async def aclose(self):
                self.is_closed = True

        async def main():
            with patch("samtale.agent.httpx.AsyncClient", Client):
                try:
                    with self.assertRaises(TransportError):
                        await agent.send("http://example.test", "temperature.read")
                finally:
                    await agent.close()

        asyncio.run(main())

    def test_send_rejects_mismatched_error_reply(self):
        agent = Agent("controller")

        class Response:
            status_code = 500

            def __init__(self, data):
                self._data = data
                self.request = httpx.Request("POST", "http://example.test/messages")

            def raise_for_status(self):
                raise httpx.HTTPStatusError("server error", request=self.request, response=self)

            def json(self):
                return self._data

        class Client:
            def __init__(self, **kwargs):
                self.is_closed = False

            async def post(self, url, json):
                wrong = Message(sender="controller", type="temperature.read")
                return Response(Message.error("temperature", wrong, "handler_failure", {"message": "The message handler failed"}).to_dict())

            async def aclose(self):
                self.is_closed = True

        async def main():
            with patch("samtale.agent.httpx.AsyncClient", Client):
                try:
                    with self.assertRaises(TransportError):
                        await agent.send("http://example.test", "temperature.read")
                finally:
                    await agent.close()

        asyncio.run(main())

    def test_send_rejects_malformed_remote_response(self):
        agent = Agent("controller")

        class Response:
            status_code = 200

            def raise_for_status(self):
                pass

            def json(self):
                return {"not": "a message"}

        class Client:
            def __init__(self, **kwargs):
                self.is_closed = False

            async def post(self, url, json):
                return Response()

            async def aclose(self):
                self.is_closed = True

        async def main():
            with patch("samtale.agent.httpx.AsyncClient", Client):
                try:
                    with self.assertRaises(TransportError):
                        await agent.send("http://example.test", "temperature.read")
                finally:
                    await agent.close()

        asyncio.run(main())

    def test_shutdown_returns_agent_closed_to_active_and_queued_requests(self):
        async def main():
            agent = Agent("temperature")
            entered = asyncio.Event()

            @agent.on("temperature.read")
            async def read(message):
                entered.set()
                await asyncio.Event().wait()

            await agent.start()
            active = asyncio.create_task(
                call_app(
                    agent.app,
                    "/messages",
                    Message(sender="controller", type="temperature.read").to_dict(),
                )
            )
            await entered.wait()
            queued = asyncio.create_task(
                call_app(
                    agent.app,
                    "/messages",
                    Message(sender="controller", type="temperature.read").to_dict(),
                )
            )
            while agent.inbox.qsize() == 0:
                await asyncio.sleep(0)

            await agent.close()
            return await asyncio.gather(active, queued)

        responses = asyncio.run(main())
        self.assertEqual([response["status"] for response in responses], [503, 503])
        self.assertEqual(
            [response["json"]["reason"] for response in responses],
            ["agent_closed", "agent_closed"],
        )

    def test_max_concurrency_bounds_simultaneous_handlers(self):
        async def main():
            agent = Agent("temperature", max_concurrency=2)
            release = asyncio.Event()
            two_started = asyncio.Event()
            running = 0
            max_running = 0
            started = 0

            @agent.on("temperature.read")
            async def read(message):
                nonlocal running, max_running, started
                running += 1
                started += 1
                max_running = max(max_running, running)
                if started == 2:
                    two_started.set()
                await release.wait()
                running -= 1
                return {"value": 18.4}

            await agent.start()
            requests = [
                asyncio.create_task(
                    call_app(
                        agent.app,
                        "/messages",
                        Message(sender="controller", type="temperature.read").to_dict(),
                    )
                )
                for _ in range(3)
            ]
            await two_started.wait()
            await asyncio.sleep(0)
            observed_before_release = started
            release.set()
            try:
                responses = await asyncio.gather(*requests)
            finally:
                await agent.close()
            return observed_before_release, max_running, responses

        started, max_running, responses = asyncio.run(main())
        self.assertEqual(started, 2)
        self.assertEqual(max_running, 2)
        self.assertEqual([response["status"] for response in responses], [200, 200, 200])

    def test_concurrent_agent_can_service_callback_while_handler_waits(self):
        async def main():
            agent = Agent("temperature", max_concurrency=2)

            @agent.on("temperature.inner")
            async def inner(message):
                return {"value": 18.4}

            @agent.on("temperature.outer")
            async def outer(message):
                callback = await call_app(
                    agent.app,
                    "/messages",
                    Message(sender="peer", type="temperature.inner").to_dict(),
                )
                return callback["json"]["payload"]

            await agent.start()
            try:
                return await asyncio.wait_for(
                    call_app(
                        agent.app,
                        "/messages",
                        Message(sender="controller", type="temperature.outer").to_dict(),
                    ),
                    timeout=1,
                )
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 200)
        self.assertEqual(response["json"]["payload"], {"value": 18.4})

    def test_repeated_start_and_close_are_safe(self):
        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.read")
            async def read(message):
                return {"value": 18.4}

            await agent.start()
            await agent.start()
            response = await call_app(
                agent.app,
                "/messages",
                Message(sender="controller", type="temperature.read").to_dict(),
            )
            await agent.close()
            await agent.close()
            return response

        response = asyncio.run(main())
        self.assertEqual(response["json"]["status"], "ok")

    def test_agent_can_restart_after_close(self):
        async def main():
            agent = Agent("temperature")

            @agent.on("temperature.read")
            async def read(message):
                return {"value": 18.4}

            await agent.start()
            await agent.close()
            await agent.start()
            try:
                return await call_app(
                    agent.app,
                    "/messages",
                    Message(sender="controller", type="temperature.read").to_dict(),
                )
            finally:
                await agent.close()

        response = asyncio.run(main())
        self.assertEqual(response["status"], 200)
        self.assertEqual(response["json"]["status"], "ok")


async def call_app(app, path, data):
    return await call_app_raw(app, path, json.dumps(data).encode())


async def call_app_raw(app, path, request_body):
    messages = [
        {
            "type": "http.request",
            "body": request_body,
            "more_body": False,
        }
    ]
    sent = []

    async def receive():
        return messages.pop(0)

    async def send(message):
        sent.append(message)

    await app(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [(b"content-type", b"application/json")],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
        },
        receive,
        send,
    )

    start = next(message for message in sent if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in sent if message["type"] == "http.response.body")
    return {"status": start["status"], "json": json.loads(body.decode())}


if __name__ == "__main__":
    unittest.main()
