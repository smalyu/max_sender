import unittest
from unittest.mock import AsyncMock, patch

from aiohttp import ClientError

from max_sender import MaxSender, Photo, Video


class FakeResponse:
    def __init__(
        self,
        status: int,
        json_body=None,
        *,
        headers=None,
        text_body: str = "",
        json_error: bool = False,
    ):
        self.status = status
        self._json_body = json_body if json_body is not None else {}
        self.headers = headers or {}
        self._text_body = text_body
        self._json_error = json_error

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self, content_type=None):
        if self._json_error:
            raise ValueError("invalid json")
        return self._json_body

    async def text(self):
        return self._text_body


class FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def post(self, url, *, params=None, json=None):
        self.calls.append(
            {
                "url": url,
                "params": params,
                "json": json,
            }
        )
        next_item = self._responses.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item


class FakeCollection:
    def __init__(self):
        self.documents = []

    async def insert_one(self, document):
        self.documents.append(document)


class MaxSenderPayloadTests(unittest.TestCase):
    def test_prepare_text_payload(self):
        sender = MaxSender(token="token", use_mongo=False)

        payload = sender._prepare_message_payload("hello", [], None)

        self.assertEqual(
            payload,
            {
                "text": "hello",
                "format": "html",
            },
        )

    def test_prepare_single_photo_payload(self):
        sender = MaxSender(token="token", use_mongo=False)

        payload = sender._prepare_message_payload("photo", [Photo("img-token")], None)

        self.assertEqual(payload["attachments"][0]["type"], "image")
        self.assertEqual(payload["attachments"][0]["payload"]["token"], "img-token")

    def test_prepare_single_video_payload(self):
        sender = MaxSender(token="token", use_mongo=False)

        payload = sender._prepare_message_payload("video", [Video("video-token")], None)

        self.assertEqual(payload["attachments"][0]["type"], "video")
        self.assertEqual(payload["attachments"][0]["payload"]["token"], "video-token")

    def test_preserves_media_order(self):
        sender = MaxSender(token="token", use_mongo=False)

        payload = sender._prepare_message_payload(
            "ordered",
            [Photo("img-1"), Video("vid-1"), Photo("img-2")],
            None,
        )

        self.assertEqual(
            [attachment["type"] for attachment in payload["attachments"]],
            ["image", "video", "image"],
        )
        self.assertEqual(
            [attachment["payload"]["token"] for attachment in payload["attachments"]],
            ["img-1", "vid-1", "img-2"],
        )

    def test_converts_inline_keyboard(self):
        sender = MaxSender(token="token", use_mongo=False)

        payload = sender._prepare_message_payload(
            "buttons",
            [],
            {
                "inline_keyboard": [
                    [
                        {"text": "Open", "url": "https://example.com"},
                        {"text": "Ping", "callback_data": "ping"},
                    ]
                ]
            },
        )

        self.assertEqual(
            payload["attachments"],
            [
                {
                    "type": "inline_keyboard",
                    "payload": {
                        "buttons": [
                            [
                                {
                                    "type": "link",
                                    "text": "Open",
                                    "url": "https://example.com",
                                },
                                {
                                    "type": "callback",
                                    "text": "Ping",
                                    "payload": "ping",
                                },
                            ]
                        ]
                    },
                }
            ],
        )

    def test_rejects_unsupported_parse_mode(self):
        with self.assertRaisesRegex(ValueError, "Unsupported parse_mode"):
            MaxSender(token="token", use_mongo=False, parse_mode="MarkdownV2")

    def test_caps_batch_size_to_platform_rate_limit(self):
        sender = MaxSender(token="token", use_mongo=False, batch_size=180)

        batches = list(
            sender._create_send_batches(
                payload={"text": "hello", "format": "html"},
                query_params={"disable_link_preview": "false"},
                recipient_ids=list(range(61)),
                recipient_type="user",
            )
        )

        self.assertEqual([len(batch) for batch in batches], [25, 25, 11])

    def test_batch_interval_uses_rate_limit_floor(self):
        sender = MaxSender(
            token="token",
            use_mongo=False,
            batch_size=180,
            delay_between_batches=0.0,
        )

        self.assertEqual(sender._get_batch_interval(25), 1.1)

    def test_batch_interval_preserves_user_delay_when_higher(self):
        sender = MaxSender(
            token="token",
            use_mongo=False,
            batch_size=25,
            delay_between_batches=1.1,
        )

        self.assertEqual(sender._get_batch_interval(25), 1.1)


class MaxSenderDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sender = MaxSender(token="token", use_mongo=False)

    async def test_uses_user_id_query_param(self):
        session = FakeSession([FakeResponse(200, {"message": {"id": "1"}})])
        self.sender._session = session

        delivered = await self.sender._send_message(
            {
                "payload": {"text": "hello", "format": "html"},
                "query_params": {"disable_link_preview": False},
                "recipient_id": 4600731,
                "recipient_type": "user",
            }
        )

        self.assertTrue(delivered)
        self.assertEqual(session.calls[0]["params"]["user_id"], 4600731)
        self.assertNotIn("chat_id", session.calls[0]["params"])

    async def test_uses_chat_id_query_param(self):
        session = FakeSession([FakeResponse(200, {"message": {"id": "1"}})])
        self.sender._session = session

        delivered = await self.sender._send_message(
            {
                "payload": {"text": "hello", "format": "html"},
                "query_params": {"disable_link_preview": False},
                "recipient_id": 42,
                "recipient_type": "chat",
            }
        )

        self.assertTrue(delivered)
        self.assertEqual(session.calls[0]["params"]["chat_id"], 42)
        self.assertNotIn("user_id", session.calls[0]["params"])

    async def test_retries_http_429_with_server_hint(self):
        self.sender._session = FakeSession(
            [
                FakeResponse(429, {"retry_after": 0.25}),
                FakeResponse(200, {"message": {"id": "2"}}),
            ]
        )

        with patch(
            "max_sender.core.asyncio.sleep", new_callable=AsyncMock
        ) as sleep_mock:
            delivered = await self.sender._send_message(
                {
                    "payload": {"text": "hello", "format": "html"},
                    "query_params": {"disable_link_preview": False},
                    "recipient_id": 1,
                    "recipient_type": "user",
                }
            )

        self.assertTrue(delivered)
        sleep_mock.assert_awaited_once_with(0.25)

    async def test_retries_http_503(self):
        self.sender._session = FakeSession(
            [
                FakeResponse(503, {"message": "unavailable"}),
                FakeResponse(200, {"message": {"id": "3"}}),
            ]
        )

        with patch(
            "max_sender.core.asyncio.sleep", new_callable=AsyncMock
        ) as sleep_mock:
            delivered = await self.sender._send_message(
                {
                    "payload": {"text": "hello", "format": "html"},
                    "query_params": {"disable_link_preview": False},
                    "recipient_id": 1,
                    "recipient_type": "user",
                }
            )

        self.assertTrue(delivered)
        sleep_mock.assert_awaited_once_with(1.0)

    async def test_retries_network_exception(self):
        self.sender._session = FakeSession(
            [
                ClientError("network down"),
                FakeResponse(200, {"message": {"id": "4"}}),
            ]
        )

        with patch(
            "max_sender.core.asyncio.sleep", new_callable=AsyncMock
        ) as sleep_mock:
            delivered = await self.sender._send_message(
                {
                    "payload": {"text": "hello", "format": "html"},
                    "query_params": {"disable_link_preview": False},
                    "recipient_id": 1,
                    "recipient_type": "user",
                }
            )

        self.assertTrue(delivered)
        sleep_mock.assert_awaited_once_with(1.0)

    async def test_retries_attachment_not_ready(self):
        self.sender._session = FakeSession(
            [
                FakeResponse(
                    400,
                    {
                        "code": "attachment.not.ready",
                        "message": "Key: errors.process.attachment.file.not.processed",
                    },
                ),
                FakeResponse(200, {"message": {"id": "5"}}),
            ]
        )

        with patch(
            "max_sender.core.asyncio.sleep", new_callable=AsyncMock
        ) as sleep_mock:
            delivered = await self.sender._send_message(
                {
                    "payload": {
                        "attachments": [{"type": "image", "payload": {"token": "img"}}]
                    },
                    "query_params": {"disable_link_preview": False},
                    "recipient_id": 1,
                    "recipient_type": "user",
                }
            )

        self.assertTrue(delivered)
        sleep_mock.assert_awaited_once_with(1.0)

    async def test_does_not_retry_permanent_4xx(self):
        self.sender._session = FakeSession(
            [FakeResponse(400, {"code": "bad.request", "message": "bad"})]
        )

        with patch(
            "max_sender.core.asyncio.sleep", new_callable=AsyncMock
        ) as sleep_mock:
            delivered = await self.sender._send_message(
                {
                    "payload": {"text": "hello", "format": "html"},
                    "query_params": {"disable_link_preview": False},
                    "recipient_id": 1,
                    "recipient_type": "user",
                }
            )

        self.assertFalse(delivered)
        sleep_mock.assert_not_awaited()

    async def test_pauses_sender_and_retries_queue_before_later_batches(self):
        sender = MaxSender(
            token="token",
            use_mongo=False,
            batch_size=1,
            delay_between_batches=0.0,
        )
        session = FakeSession(
            [
                FakeResponse(429, {"retry_after": 0.25}),
                FakeResponse(200, {"message": {"id": "7"}}),
                FakeResponse(200, {"message": {"id": "8"}}),
            ]
        )
        sender._session = session

        with (
            patch(
                "max_sender.core.asyncio.sleep", new_callable=AsyncMock
            ) as sleep_mock,
            patch.object(sender, "_get_batch_interval", return_value=0.0),
        ):
            delivered, not_delivered = await sender._send_messages(
                payload={"text": "hello", "format": "html"},
                query_params={"disable_link_preview": False},
                recipient_ids=[1, 2],
                recipient_type="user",
            )

        self.assertEqual((delivered, not_delivered), (2, 0))
        self.assertEqual(
            [call["params"]["user_id"] for call in session.calls],
            [1, 1, 2],
        )
        sleep_mock.assert_awaited_once_with(0.25)

    async def test_exhausts_rate_limit_retries_after_max_attempts(self):
        self.sender._session = FakeSession(
            [
                FakeResponse(429, {"message": "slow down"}),
                FakeResponse(429, {"message": "slow down"}),
                FakeResponse(429, {"message": "slow down"}),
                FakeResponse(429, {"message": "slow down"}),
                FakeResponse(429, {"message": "slow down"}),
                FakeResponse(429, {"message": "slow down"}),
            ]
        )

        with patch(
            "max_sender.core.asyncio.sleep", new_callable=AsyncMock
        ) as sleep_mock:
            delivered = await self.sender._send_message(
                {
                    "payload": {"text": "hello", "format": "html"},
                    "query_params": {"disable_link_preview": False},
                    "recipient_id": 1,
                    "recipient_type": "user",
                }
            )

        self.assertFalse(delivered)
        self.assertEqual(len(self.sender._session.calls), 6)
        self.assertEqual(
            [args.args[0] for args in sleep_mock.await_args_list],
            [4.0, 8.0, 16.0, 32.0, 64.0],
        )

    async def test_logs_successful_attempt_to_mongo(self):
        self.sender._use_mongo = True
        self.sender._mongo_collection = FakeCollection()
        self.sender._session = FakeSession(
            [FakeResponse(200, {"message": {"id": "6"}})]
        )

        delivered = await self.sender._send_message(
            {
                "payload": {"text": "hello", "format": "html"},
                "query_params": {"disable_link_preview": True},
                "recipient_id": 1,
                "recipient_type": "user",
            }
        )

        self.assertTrue(delivered)
        self.assertEqual(len(self.sender._mongo_collection.documents), 1)
        document = self.sender._mongo_collection.documents[0]
        self.assertTrue(document["delivered"])
        self.assertEqual(document["recipient_id"], 1)
        self.assertEqual(document["payload_summary"]["disable_link_preview"], True)

    async def test_logs_failed_attempt_to_mongo(self):
        self.sender._use_mongo = True
        self.sender._mongo_collection = FakeCollection()
        self.sender._session = FakeSession(
            [FakeResponse(400, {"code": "bad.request", "message": "bad"})]
        )

        delivered = await self.sender._send_message(
            {
                "payload": {"text": "hello", "format": "html"},
                "query_params": {"disable_link_preview": False},
                "recipient_id": 2,
                "recipient_type": "user",
            }
        )

        self.assertFalse(delivered)
        self.assertEqual(len(self.sender._mongo_collection.documents), 1)
        document = self.sender._mongo_collection.documents[0]
        self.assertFalse(document["delivered"])
        self.assertEqual(document["http_status"], 400)
        self.assertEqual(document["error_code"], "bad.request")

    async def test_logs_retry_attempts_with_will_retry_flag(self):
        self.sender._use_mongo = True
        self.sender._mongo_collection = FakeCollection()
        self.sender._session = FakeSession(
            [
                FakeResponse(429, {"retry_after": 0.25}),
                FakeResponse(200, {"message": {"id": "9"}}),
            ]
        )

        with patch("max_sender.core.asyncio.sleep", new_callable=AsyncMock):
            delivered = await self.sender._send_message(
                {
                    "payload": {"text": "hello", "format": "html"},
                    "query_params": {"disable_link_preview": False},
                    "recipient_id": 3,
                    "recipient_type": "user",
                }
            )

        self.assertTrue(delivered)
        self.assertEqual(len(self.sender._mongo_collection.documents), 2)
        first_attempt, second_attempt = self.sender._mongo_collection.documents
        self.assertTrue(first_attempt["temporary_error"])
        self.assertTrue(first_attempt["will_retry"])
        self.assertEqual(first_attempt["attempt"], 1)
        self.assertTrue(second_attempt["delivered"])
        self.assertEqual(second_attempt["attempt"], 2)


if __name__ == "__main__":
    unittest.main()
