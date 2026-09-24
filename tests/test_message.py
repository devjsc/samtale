import unittest

from samtale import Message, MessageValidationError, Status


class MessageTests(unittest.TestCase):
    def test_request_round_trip(self):
        message = Message(sender="controller", type="temperature.read", payload={"unit": "c"})

        self.assertEqual(Message.from_dict(message.to_dict()), message)

    def test_response_constructors_round_trip(self):
        request = Message(sender="controller", type="temperature.set", payload={"value": 24})

        ok = Message.ok("temperature", request, {"value": 24})
        rejected = Message.reject("temperature", request, "outside_supported_range", {"maximum": 22})
        error = Message.error("temperature", request, "handler_failure", {"message": "The message handler failed"})

        self.assertEqual(Message.from_dict(ok.to_dict()), ok)
        self.assertEqual(Message.from_dict(rejected.to_dict()), rejected)
        self.assertEqual(Message.from_dict(error.to_dict()), error)
        self.assertEqual(ok.type, "temperature.set")
        self.assertIs(ok.status, Status.OK)
        self.assertIs(rejected.status, Status.REJECTED)
        self.assertIs(error.status, Status.ERROR)

    def test_uncorrelated_framework_error_round_trip(self):
        error = Message.error("temperature", None, "malformed_message", {"message": "missing required field: type"})

        self.assertEqual(error.type, "_agent.error")
        self.assertIs(error.status, Status.ERROR)
        self.assertEqual(Message.from_dict(error.to_dict()), error)

    def test_message_defaults_payload(self):
        message = Message.from_dict({"id": "1", "sender": "a", "type": "read"})

        self.assertEqual(message.payload, {})

    def test_request_cannot_have_status(self):
        self.assert_invalid({"id": "1", "sender": "a", "type": "read", "status": "ok"})

    def test_response_requires_status(self):
        self.assert_invalid({"id": "1", "sender": "a", "type": "read", "reply_to": "r1"})

    def test_rejection_requires_reason(self):
        self.assert_invalid(
            {"id": "1", "sender": "a", "type": "read", "reply_to": "r1", "status": "rejected"}
        )

    def test_successful_response_cannot_have_reason(self):
        self.assert_invalid(
            {
                "id": "1",
                "sender": "a",
                "type": "read",
                "reply_to": "r1",
                "status": "ok",
                "reason": "unexpected",
            }
        )

    def test_unknown_fields_are_rejected(self):
        self.assert_invalid({"id": "1", "sender": "a", "type": "read", "unknown": True})

    def test_invalid_status_is_rejected(self):
        self.assert_invalid(
            {"id": "1", "sender": "a", "type": "read", "reply_to": "r1", "status": "missing"}
        )

    def test_payload_must_be_an_object(self):
        self.assert_invalid({"id": "1", "sender": "a", "type": "read", "payload": []})

    def test_payload_must_be_json_compatible(self):
        with self.assertRaises(MessageValidationError):
            Message(sender="a", type="read", payload={"value": object()})
        with self.assertRaises(MessageValidationError):
            Message(sender="a", type="read", payload={"value": float("nan")})

    def test_required_envelope_fields_are_validated(self):
        bad_messages = [
            None,
            {"sender": "a", "type": "read"},
            {"id": None, "sender": "a", "type": "read"},
            {"id": "1", "sender": None, "type": "read"},
            {"id": "1", "sender": "a", "type": ""},
            {"id": "1", "sender": "a", "type": "read", "reply_to": None},
            {"id": "1", "sender": "a", "type": "read", "reason": "nope"},
            {"id": "1", "sender": "a", "type": "read", "reply_to": "r1", "status": "error", "reason": ""},
        ]
        for data in bad_messages:
            with self.subTest(data=data):
                self.assert_invalid(data)

    def assert_invalid(self, data):
        with self.assertRaises(MessageValidationError):
            Message.from_dict(data)


if __name__ == "__main__":
    unittest.main()
