import asyncio
import logging
import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from aiohttp import ClientError, ClientSession
from pydantic import BaseModel
from pymongo import AsyncMongoClient

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

if TYPE_CHECKING:
    from pymongo.asynchronous.collection import AsyncCollection


class MediaItem(BaseModel):
    type: Literal["image", "video"]
    media: str


class Photo(MediaItem):
    type: Literal["image"] = "image"

    def __init__(self, file_token: str):
        super().__init__(media=file_token, type="image")


class Video(MediaItem):
    type: Literal["video"] = "video"

    def __init__(self, file_token: str):
        super().__init__(media=file_token, type="video")


@dataclass
class SendAttemptResult:
    request_data: dict[str, Any]
    delivered: bool
    can_retry: bool
    retry_delay: float | None = None


class MaxSender:
    _url: str = "https://platform-api.max.ru/messages"
    _parse_modes = {
        "Markdown": "markdown",
        "HTML": "html",
    }
    _max_requests_per_second = 30
    _safe_batch_size = 25
    _safe_delay_between_batches = 1.1
    _temporary_statuses = {429, 503}
    _temporary_codes = {"attachment.not.ready"}
    _retry_attempts = 3
    _retry_base_delay = 1.0
    _rate_limit_retry_attempts = 5
    _rate_limit_retry_base_delay = 4.0

    def __init__(
        self,
        token: str,
        batch_size: int = 25,
        delay_between_batches: float = 1.1,
        use_mongo: bool = True,
        mongo_uri: str = "mongodb://localhost:27017",
        mongo_db: str = "max_sender",
        parse_mode: Literal["Markdown", "HTML"] = "HTML",
    ):
        if batch_size < 1:
            raise ValueError("batch_size must be greater than 0")

        self._token = token
        self._batch_size = batch_size
        self._delay_between_batches = delay_between_batches
        self._use_mongo = use_mongo
        self._mongo_uri = mongo_uri
        self._mongo_db = mongo_db
        self._parse_mode = self._normalize_parse_mode(parse_mode)
        self._mongo_collection: AsyncCollection | None = None
        self._session: ClientSession | None = None

    async def run(
        self,
        recipient_ids: list[int],
        text: str | None = "",
        media_items: list[MediaItem] | None = None,
        reply_markup: dict | None = None,
        disable_web_page_preview: bool = False,
        recipient_type: Literal["user", "chat"] = "user",
    ) -> tuple[int, int]:
        """Runs the sending process and returns (delivered, not_delivered)."""
        if media_items is None:
            media_items = []

        if recipient_type not in {"user", "chat"}:
            raise ValueError("recipient_type must be either 'user' or 'chat'")

        payload = self._prepare_message_payload(text, media_items, reply_markup)
        query_params = self._prepare_query_params(disable_web_page_preview)

        mongo_client: AsyncMongoClient | None = None
        async with ClientSession(
            headers={"Authorization": self._token, "Content-Type": "application/json"}
        ) as self._session:
            try:
                if self._use_mongo:
                    mongo_client = AsyncMongoClient(self._mongo_uri)
                    collection_name = self._get_collection_name()
                    self._mongo_collection = mongo_client[self._mongo_db][
                        collection_name
                    ]
                return await self._send_messages(
                    payload, query_params, recipient_ids, recipient_type
                )
            finally:
                self._mongo_collection = None
                if mongo_client is not None:
                    await self._close_mongo_client(mongo_client)

    @classmethod
    def _normalize_parse_mode(cls, parse_mode: str) -> str:
        normalized = cls._parse_modes.get(parse_mode)
        if normalized is None:
            allowed_modes = ", ".join(cls._parse_modes)
            raise ValueError(
                f"Unsupported parse_mode {parse_mode!r}. Use one of: {allowed_modes}"
            )
        return normalized

    def _prepare_query_params(self, disable_web_page_preview: bool) -> dict[str, str]:
        return {"disable_link_preview": "true" if disable_web_page_preview else "false"}

    def _prepare_message_payload(
        self,
        text: str | None,
        media_items: list[MediaItem],
        reply_markup: dict | None,
    ) -> dict[str, Any]:
        text = "" if text is None else text
        attachments = self._prepare_media_attachments(media_items)

        if reply_markup is not None:
            attachments.append(self._prepare_inline_keyboard_attachment(reply_markup))

        if not text and not attachments:
            raise ValueError("text or attachments are required")

        payload: dict[str, Any] = {}
        if text:
            payload["text"] = text
            payload["format"] = self._parse_mode
        if attachments:
            payload["attachments"] = attachments
        return payload

    def _prepare_media_attachments(
        self, media_items: list[MediaItem]
    ) -> list[dict[str, Any]]:
        return [
            {
                "type": item.type,
                "payload": {"token": item.media},
            }
            for item in media_items
        ]

    def _prepare_inline_keyboard_attachment(
        self, reply_markup: dict[str, Any]
    ) -> dict[str, Any]:
        keyboard = (
            reply_markup.get("inline_keyboard")
            if isinstance(reply_markup, dict)
            else None
        )
        if not isinstance(keyboard, list) or not keyboard:
            raise ValueError(
                "reply_markup must contain a non-empty inline_keyboard list"
            )

        rows: list[list[dict[str, Any]]] = []
        for row in keyboard:
            if not isinstance(row, list) or not row:
                raise ValueError("Each inline keyboard row must be a non-empty list")

            converted_row: list[dict[str, Any]] = []
            for button in row:
                if not isinstance(button, dict):
                    raise ValueError("Each inline keyboard button must be a dictionary")

                text = button.get("text")
                if not text:
                    raise ValueError("Each inline keyboard button must contain text")

                if "url" in button:
                    converted_row.append(
                        {
                            "type": "link",
                            "text": text,
                            "url": button["url"],
                        }
                    )
                elif "callback_data" in button:
                    converted_row.append(
                        {
                            "type": "callback",
                            "text": text,
                            "payload": button["callback_data"],
                        }
                    )
                else:
                    raise ValueError(
                        "Only inline keyboard buttons with url or callback_data are supported in MAX"
                    )
            rows.append(converted_row)

        return {
            "type": "inline_keyboard",
            "payload": {"buttons": rows},
        }

    def _get_collection_name(self) -> str:
        """Generates a unique MongoDB collection name using Moscow time."""
        moscow_time = time.gmtime(time.time() + 3 * 60 * 60)
        return time.strftime("%d_%m_%Y__%H_%M_%S", moscow_time)

    async def _send_messages(
        self,
        payload: dict[str, Any],
        query_params: dict[str, str],
        recipient_ids: list[int],
        recipient_type: Literal["user", "chat"],
    ) -> tuple[int, int]:
        batches = self._create_send_batches(
            payload, query_params, recipient_ids, recipient_type
        )
        return await self._execute_batches(batches)

    def _create_send_batches(
        self,
        payload: dict[str, Any],
        query_params: dict[str, str],
        recipient_ids: list[int],
        recipient_type: Literal["user", "chat"],
    ) -> Generator[list[dict[str, Any]], None, None]:
        effective_batch_size = self._get_effective_batch_size()
        batch: list[dict[str, Any]] = []
        for recipient_id in recipient_ids:
            batch.append(
                {
                    "payload": payload,
                    "query_params": query_params,
                    "recipient_id": recipient_id,
                    "recipient_type": recipient_type,
                    "attempt": 1,
                }
            )
            if len(batch) >= effective_batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _get_effective_batch_size(self) -> int:
        return min(self._batch_size, self._safe_batch_size)

    def _get_batch_interval(self, batch_length: int) -> float:
        rate_limit_interval = batch_length / self._max_requests_per_second
        return max(
            self._delay_between_batches,
            self._safe_delay_between_batches,
            rate_limit_interval,
        )

    async def _send_message(self, request_data: dict[str, Any]) -> bool:
        normalized_request = self._normalize_request_data(request_data)
        (
            delivered,
            not_delivered,
            retry_queue,
            retry_delay,
        ) = await self._process_batch_messages([normalized_request])
        if retry_queue:
            retried_delivered, retried_not_delivered = await self._drain_retry_queue(
                retry_queue, retry_delay
            )
            delivered += retried_delivered
            not_delivered += retried_not_delivered
        return delivered == 1 and not_delivered == 0

    def _normalize_request_data(self, request_data: dict[str, Any]) -> dict[str, Any]:
        normalized_request = dict(request_data)
        normalized_request.setdefault("attempt", 1)
        return normalized_request

    def _chunk_request_batch(
        self, request_data_list: list[dict[str, Any]]
    ) -> Generator[list[dict[str, Any]], None, None]:
        effective_batch_size = self._get_effective_batch_size()
        for index in range(0, len(request_data_list), effective_batch_size):
            yield request_data_list[index : index + effective_batch_size]

    async def _send_message_once(
        self, request_data: dict[str, Any]
    ) -> SendAttemptResult:
        if self._session is None:
            raise RuntimeError("Session is not initialized")

        recipient_id = request_data["recipient_id"]
        recipient_type = request_data["recipient_type"]
        payload = request_data["payload"]
        query_params = request_data["query_params"]
        attempt = request_data["attempt"]
        response_status: int | None = None
        response_body: Any = None
        response_headers: dict[str, str] = {}
        exception: Exception | None = None
        temporary_error = False
        retry_after: float | None = None

        try:
            params = dict(query_params)
            params[f"{recipient_type}_id"] = recipient_id
            async with self._session.post(
                self._url,
                params=params,
                json=payload,
            ) as response:
                response_status = response.status
                response_headers = dict(response.headers)
                response_body = await self._parse_response_body(response)

            delivered = response_status == 200
            if delivered:
                logger.info("Delivered to %s_id=%s", recipient_type, recipient_id)
            else:
                temporary_error, retry_after = self._should_retry(
                    response_status, response_body, response_headers
                )
                log_method = logger.warning if temporary_error else logger.error
                log_method(
                    "Failed for %s_id=%s on attempt %s: status=%s body=%s",
                    recipient_type,
                    recipient_id,
                    attempt,
                    response_status,
                    response_body,
                )
        except (ClientError, asyncio.TimeoutError, OSError) as exc:
            delivered = False
            temporary_error = True
            exception = exc
            logger.warning(
                "Temporary exception for %s_id=%s on attempt %s: %s",
                recipient_type,
                recipient_id,
                attempt,
                exc,
            )
        except Exception as exc:
            delivered = False
            exception = exc
            logger.exception(
                "Unexpected exception for %s_id=%s on attempt %s",
                recipient_type,
                recipient_id,
                attempt,
            )

        retry_category = self._get_retry_category(
            temporary_error, response_status, response_body
        )
        max_attempts = self._get_max_attempts(retry_category)
        can_retry = temporary_error and attempt < max_attempts and not delivered
        retry_delay = (
            self._get_retry_delay(attempt, retry_after, retry_category)
            if can_retry
            else None
        )
        await self._log_attempt(
            recipient_id=recipient_id,
            recipient_type=recipient_type,
            payload=payload,
            query_params=query_params,
            attempt=attempt,
            delivered=delivered,
            response_status=response_status,
            response_body=response_body,
            exception=exception,
            temporary_error=temporary_error,
            retry_after=retry_after,
            will_retry=can_retry,
        )
        return SendAttemptResult(
            request_data=request_data,
            delivered=delivered,
            can_retry=can_retry,
            retry_delay=retry_delay,
        )

    async def _process_batch_messages(
        self,
        batch: list[dict[str, Any]],
    ) -> tuple[int, int, list[dict[str, Any]], float]:
        delivered, not_delivered = 0, 0
        retry_queue: list[dict[str, Any]] = []
        retry_delay = 0.0
        results = await asyncio.gather(
            *(self._send_message_once(request_data) for request_data in batch)
        )
        for result in results:
            if result.delivered:
                delivered += 1
                continue

            if result.can_retry:
                retry_request = dict(result.request_data)
                retry_request["attempt"] = result.request_data["attempt"] + 1
                retry_queue.append(retry_request)
                retry_delay = max(retry_delay, result.retry_delay or 0.0)
                continue

            not_delivered += 1
        return delivered, not_delivered, retry_queue, retry_delay

    async def _drain_retry_queue(
        self,
        retry_queue: list[dict[str, Any]],
        retry_delay: float,
    ) -> tuple[int, int]:
        total_delivered, total_not_delivered = 0, 0
        pending_requests = retry_queue
        pending_delay = retry_delay

        while pending_requests:
            if pending_delay > 0:
                logger.info(
                    "Pausing broadcast for %.2fs before retrying %s temporary failures",
                    pending_delay,
                    len(pending_requests),
                )
                await asyncio.sleep(pending_delay)

            current_requests = pending_requests
            pending_requests = []
            pending_delay = 0.0
            sleep_time = 0.0

            for batch in self._chunk_request_batch(current_requests):
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

                batch_start_time = time.monotonic()
                (
                    delivered,
                    not_delivered,
                    retry_batch,
                    retry_batch_delay,
                ) = await self._process_batch_messages(batch)
                total_delivered += delivered
                total_not_delivered += not_delivered
                pending_requests.extend(retry_batch)
                pending_delay = max(pending_delay, retry_batch_delay)

                batch_interval = self._get_batch_interval(len(batch))
                sleep_time = max(
                    batch_start_time + batch_interval - time.monotonic(),
                    0.0,
                )

        return total_delivered, total_not_delivered

    async def _execute_batches(
        self,
        batches: Generator[list[dict[str, Any]], None, None],
    ) -> tuple[int, int]:
        total_delivered, total_not_delivered = 0, 0
        sleep_time = 0.0

        for batch in batches:
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

            batch_start_time = time.monotonic()
            (
                delivered,
                not_delivered,
                retry_queue,
                retry_delay,
            ) = await self._process_batch_messages(batch)
            total_delivered += delivered
            total_not_delivered += not_delivered
            if retry_queue:
                (
                    retried_delivered,
                    retried_not_delivered,
                ) = await self._drain_retry_queue(retry_queue, retry_delay)
                total_delivered += retried_delivered
                total_not_delivered += retried_not_delivered

            batch_interval = self._get_batch_interval(len(batch))
            sleep_time = max(
                batch_start_time + batch_interval - time.monotonic(),
                0.0,
            )

        return total_delivered, total_not_delivered

    async def _parse_response_body(self, response: Any) -> Any:
        try:
            return await response.json(content_type=None)
        except Exception:
            text = await response.text()
            return {"raw_text": text} if text else {}

    def _should_retry(
        self,
        response_status: int | None,
        response_body: Any,
        response_headers: dict[str, str],
    ) -> tuple[bool, float | None]:
        if response_status in self._temporary_statuses:
            return True, self._extract_retry_after(response_body, response_headers)

        if isinstance(response_body, dict):
            error_code = response_body.get("code")
            if error_code in self._temporary_codes:
                return True, self._extract_retry_after(response_body, response_headers)

        return False, None

    def _extract_retry_after(
        self,
        response_body: Any,
        response_headers: dict[str, str],
    ) -> float | None:
        candidates: list[Any] = [response_headers.get("Retry-After")]
        if isinstance(response_body, dict):
            candidates.append(response_body.get("retry_after"))

            parameters = response_body.get("parameters")
            if isinstance(parameters, dict):
                candidates.append(parameters.get("retry_after"))

            details = response_body.get("details")
            if isinstance(details, dict):
                candidates.append(details.get("retry_after"))

        for value in candidates:
            try:
                if value is None:
                    continue
                parsed_value = float(value)
            except (TypeError, ValueError):
                continue
            if parsed_value > 0:
                return parsed_value
        return None

    def _get_retry_category(
        self,
        temporary_error: bool,
        response_status: int | None,
        response_body: Any,
    ) -> Literal["rate_limit", "temporary"] | None:
        if not temporary_error:
            return None

        if response_status == 429:
            return "rate_limit"

        if (
            isinstance(response_body, dict)
            and response_body.get("code") == "too.many.requests"
        ):
            return "rate_limit"

        return "temporary"

    def _get_max_attempts(
        self, retry_category: Literal["rate_limit", "temporary"] | None
    ) -> int:
        if retry_category == "rate_limit":
            return self._rate_limit_retry_attempts + 1
        return self._retry_attempts + 1

    def _get_retry_delay(
        self,
        attempt: int,
        retry_after: float | None,
        retry_category: Literal["rate_limit", "temporary"] | None = None,
    ) -> float:
        if retry_after is not None:
            return retry_after
        base_delay = (
            self._rate_limit_retry_base_delay
            if retry_category == "rate_limit"
            else self._retry_base_delay
        )
        return base_delay * (2 ** (attempt - 1))

    async def _log_attempt(
        self,
        *,
        recipient_id: int,
        recipient_type: Literal["user", "chat"],
        payload: dict[str, Any],
        query_params: dict[str, str],
        attempt: int,
        delivered: bool,
        response_status: int | None,
        response_body: Any,
        exception: Exception | None,
        temporary_error: bool,
        retry_after: float | None,
        will_retry: bool,
    ) -> None:
        if not self._use_mongo or self._mongo_collection is None:
            return

        document = {
            "created_at": time.time(),
            "recipient_id": recipient_id,
            "recipient_type": recipient_type,
            "attempt": attempt,
            "delivered": delivered,
            "http_status": response_status,
            "response": response_body,
            "error_code": response_body.get("code")
            if isinstance(response_body, dict)
            else None,
            "exception": repr(exception) if exception is not None else None,
            "temporary_error": temporary_error,
            "retry_after": retry_after,
            "will_retry": will_retry,
            "payload_summary": self._build_payload_summary(payload, query_params),
        }

        try:
            await self._mongo_collection.insert_one(document)
        except Exception:
            logger.exception(
                "Failed to write MongoDB log for recipient_id=%s", recipient_id
            )

    def _build_payload_summary(
        self,
        payload: dict[str, Any],
        query_params: dict[str, str],
    ) -> dict[str, Any]:
        attachment_types = [
            attachment.get("type")
            for attachment in payload.get("attachments", [])
            if isinstance(attachment, dict)
        ]

        return {
            "has_text": "text" in payload,
            "text_length": len(payload.get("text", "")),
            "format": payload.get("format"),
            "attachment_types": attachment_types,
            "inline_button_count": self._count_inline_buttons(
                payload.get("attachments", [])
            ),
            "disable_link_preview": query_params.get("disable_link_preview")
            in {True, "true"},
        }

    def _count_inline_buttons(self, attachments: list[Any]) -> int:
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            if attachment.get("type") != "inline_keyboard":
                continue

            payload = attachment.get("payload", {})
            buttons = payload.get("buttons", [])
            return sum(len(row) for row in buttons if isinstance(row, list))
        return 0

    async def _close_mongo_client(self, client: AsyncMongoClient) -> None:
        aclose = getattr(client, "aclose", None)
        if callable(aclose):
            await aclose()
            return

        close = getattr(client, "close", None)
        if callable(close):
            close()
