import argparse
import asyncio
import json
import os
import re
import socket
import sys
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aiohttp import ClientSession, FormData, web
from pymongo import MongoClient

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]

import max_sender  # noqa: E402
from max_sender import MaxSender, Photo, Video  # noqa: E402


IMAGE_URLS = (
    "https://raw.githubusercontent.com/github/explore/main/topics/python/python.png",
    "https://picsum.photos/seed/max-sender/400/300",
)
VIDEO_URLS = (
    "https://samplelib.com/lib/preview/mp4/sample-5s.mp4",
    "https://filesamples.com/samples/video/mp4/sample_640x360.mp4",
)
COLLECTION_RE = re.compile(r"^\d{2}_\d{2}_\d{4}__\d{2}_\d{2}_\d{2}$")


class VerificationError(RuntimeError):
    pass


@dataclass
class ScenarioOutcome:
    name: str
    delivered: int
    not_delivered: int
    collection: str
    documents: int


def fail(message: str) -> None:
    raise VerificationError(message)


def ensure(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def ensure_equal(actual: Any, expected: Any, message: str) -> None:
    if actual != expected:
        fail(f"{message}: expected {expected!r}, got {actual!r}")


def print_step(message: str) -> None:
    print(f"[e2e] {message}", flush=True)


class MongoProbe:
    def __init__(self, mongo_uri: str, db_name: str):
        self._client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self._db = self._client[db_name]

    def collection_names(self) -> set[str]:
        return set(self._db.list_collection_names())

    def get_new_collection(self, before: set[str]) -> str:
        after = self.collection_names()
        new_collections = sorted(after - before)
        ensure_equal(
            len(new_collections),
            1,
            f"Expected exactly one new collection in Mongo DB {self._db.name}",
        )
        collection = new_collections[0]
        ensure(
            bool(COLLECTION_RE.fullmatch(collection)),
            f"Collection name {collection!r} does not match Moscow timestamp format",
        )
        return collection

    def documents(self, collection: str) -> list[dict[str, Any]]:
        return list(self._db[collection].find().sort("_id", 1))

    def close(self) -> None:
        self._client.close()


def verify_common_document_fields(
    document: dict[str, Any],
    *,
    recipient_id: int,
    recipient_type: str,
) -> None:
    for key in (
        "created_at",
        "recipient_id",
        "recipient_type",
        "attempt",
        "delivered",
        "http_status",
        "response",
        "error_code",
        "exception",
        "temporary_error",
        "retry_after",
        "will_retry",
        "payload_summary",
    ):
        ensure(key in document, f"Missing Mongo field {key!r} in document {document}")

    ensure_equal(document["recipient_id"], recipient_id, "Unexpected recipient_id")
    ensure_equal(
        document["recipient_type"], recipient_type, "Unexpected recipient_type"
    )
    ensure(
        isinstance(document["payload_summary"], dict), "payload_summary must be a dict"
    )


def verify_payload_summary(
    document: dict[str, Any],
    *,
    has_text: bool,
    text_length: int,
    format_value: str | None,
    attachment_types: list[str],
    inline_button_count: int,
    disable_link_preview: bool,
) -> None:
    summary = document["payload_summary"]
    ensure_equal(summary["has_text"], has_text, "Unexpected has_text")
    ensure_equal(summary["text_length"], text_length, "Unexpected text_length")
    ensure_equal(summary["format"], format_value, "Unexpected format")
    ensure_equal(
        summary["attachment_types"], attachment_types, "Unexpected attachment_types"
    )
    ensure_equal(
        summary["inline_button_count"],
        inline_button_count,
        "Unexpected inline_button_count",
    )
    ensure_equal(
        summary["disable_link_preview"],
        disable_link_preview,
        "Unexpected disable_link_preview",
    )


async def wait_for_collection_slot() -> None:
    fraction = time.time() % 1
    delay = 1.15 - fraction
    if delay < 0.15:
        delay += 1.0
    await asyncio.sleep(delay)


async def assert_separate_install() -> None:
    module_path = Path(max_sender.__file__).resolve()
    ensure(
        "site-packages" in str(module_path),
        f"Expected installed package from site-packages, got {module_path}",
    )
    print_step(f"Installed package path: {module_path}")


async def run_validation_checks() -> None:
    print_step("Running installed-package validation checks")

    try:
        MaxSender(token="dummy", use_mongo=False, parse_mode="MarkdownV2")
    except ValueError as exc:
        ensure("Unsupported parse_mode" in str(exc), "Unexpected parse_mode error text")
    else:
        fail("Unsupported parse_mode check did not raise ValueError")

    sender = MaxSender(token="dummy", use_mongo=False)
    try:
        await sender.run([1], text="", media_items=[], recipient_type="user")
    except ValueError as exc:
        ensure(
            "text or attachments are required" in str(exc),
            "Unexpected empty payload error",
        )
    else:
        fail("Empty payload check did not raise ValueError")

    try:
        await sender.run([1], text="hello", recipient_type="channel")
    except ValueError as exc:
        ensure(
            "recipient_type must be either" in str(exc),
            "Unexpected recipient_type error",
        )
    else:
        fail("Invalid recipient_type check did not raise ValueError")


async def download_first_available(
    session: ClientSession,
    urls: tuple[str, ...],
    suffix: str,
    destination: Path,
) -> Path:
    errors: list[str] = []
    for index, url in enumerate(urls, start=1):
        target = destination / f"asset_{index}{suffix}"
        try:
            async with session.get(url, timeout=60) as response:
                response.raise_for_status()
                target.write_bytes(await response.read())
            ensure(target.stat().st_size > 0, f"Downloaded empty file from {url}")
            print_step(f"Downloaded {suffix} asset from {url}")
            return target
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    fail("Failed to download test asset:\n" + "\n".join(errors))


async def upload_media(
    session: ClientSession,
    *,
    token: str,
    upload_type: str,
    file_path: Path,
) -> str:
    headers = {"Authorization": token}
    async with session.post(
        f"https://platform-api.max.ru/uploads?type={upload_type}",
        headers=headers,
        timeout=60,
    ) as response:
        response.raise_for_status()
        metadata = await response.json(content_type=None)

    upload_url = metadata["url"]
    upload_token = extract_upload_token(metadata)

    form = FormData()
    with file_path.open("rb") as file_handle:
        form.add_field("data", file_handle, filename=file_path.name)
        async with session.post(
            upload_url,
            headers=headers,
            data=form,
            timeout=120,
        ) as response:
            response.raise_for_status()
            upload_response = await parse_json_or_text(response)

    token_value = extract_upload_token(upload_response) or upload_token
    ensure(
        token_value,
        f"Upload for {upload_type} did not return a token: {upload_response}",
    )
    print_step(f"Uploaded {upload_type} asset and received reusable token")
    return token_value


def extract_upload_token(payload: Any) -> str | None:
    if isinstance(payload, dict):
        token = payload.get("token")
        if isinstance(token, str) and token:
            return token
        for value in payload.values():
            nested = extract_upload_token(value)
            if nested:
                return nested
    elif isinstance(payload, list):
        for value in payload:
            nested = extract_upload_token(value)
            if nested:
                return nested
    return None


async def parse_json_or_text(response: Any) -> Any:
    try:
        return await response.json(content_type=None)
    except Exception:
        text = await response.text()
        return {"raw_text": text} if text else {}


async def run_sender_and_capture(
    sender: MaxSender,
    mongo: MongoProbe,
    *,
    recipient_ids: list[int],
    text: str | None = "",
    media_items: list[Any] | None = None,
    reply_markup: dict[str, Any] | None = None,
    disable_web_page_preview: bool = False,
    recipient_type: str = "user",
) -> tuple[ScenarioOutcome, list[dict[str, Any]], float]:
    await wait_for_collection_slot()
    before = mongo.collection_names()
    started = time.monotonic()
    delivered, not_delivered = await sender.run(
        recipient_ids,
        text=text,
        media_items=media_items,
        reply_markup=reply_markup,
        disable_web_page_preview=disable_web_page_preview,
        recipient_type=recipient_type,
    )
    elapsed = time.monotonic() - started
    collection = mongo.get_new_collection(before)
    documents = mongo.documents(collection)
    outcome = ScenarioOutcome(
        name=text or "<attachments>",
        delivered=delivered,
        not_delivered=not_delivered,
        collection=collection,
        documents=len(documents),
    )
    return outcome, documents, elapsed


def verify_live_success_documents(
    documents: list[dict[str, Any]],
    *,
    recipient_id: int,
    expected_count: int,
    expected_text: str,
    expected_format: str | None,
    expected_attachment_types: list[str],
    expected_inline_buttons: int,
    disable_link_preview: bool,
    expected_response_text: str | None = None,
    response_text_contains: list[str] | None = None,
) -> None:
    ensure_equal(len(documents), expected_count, "Unexpected live document count")
    for document in documents:
        verify_common_document_fields(
            document,
            recipient_id=recipient_id,
            recipient_type="user",
        )
        ensure_equal(
            document["attempt"], 1, "Live happy-path should complete in one attempt"
        )
        ensure_equal(
            document["delivered"], True, "Live happy-path document must be delivered"
        )
        ensure_equal(
            document["http_status"], 200, "Live happy-path HTTP status must be 200"
        )
        ensure_equal(
            document["error_code"], None, "Live happy-path error_code must be empty"
        )
        ensure_equal(
            document["exception"], None, "Live happy-path exception must be empty"
        )
        ensure_equal(
            document["temporary_error"],
            False,
            "Live happy-path temporary_error mismatch",
        )
        ensure_equal(
            document["retry_after"], None, "Live happy-path retry_after mismatch"
        )
        ensure_equal(
            document["will_retry"], False, "Live happy-path will_retry mismatch"
        )
        verify_payload_summary(
            document,
            has_text=bool(expected_text),
            text_length=len(expected_text),
            format_value=expected_format,
            attachment_types=expected_attachment_types,
            inline_button_count=expected_inline_buttons,
            disable_link_preview=disable_link_preview,
        )
        response = document["response"]
        ensure(isinstance(response, dict), "MAX response must be a dict")
        message = response.get("message")
        ensure(isinstance(message, dict), "MAX response must contain message object")
        body = message.get("body")
        ensure(isinstance(body, dict), "MAX response must contain message body")
        response_text = body.get("text")
        ensure(isinstance(response_text, str), "Live response text must be a string")
        if expected_response_text is not None:
            ensure_equal(
                response_text, expected_response_text, "Unexpected live response text"
            )
        if response_text_contains is not None:
            for fragment in response_text_contains:
                ensure(
                    fragment in response_text,
                    f"Live response text {response_text!r} does not contain {fragment!r}",
                )
        if expected_attachment_types:
            attachments = body.get("attachments", [])
            ensure(
                isinstance(attachments, list),
                "Live response attachments must be a list",
            )
            ensure_equal(
                [attachment.get("type") for attachment in attachments],
                expected_attachment_types,
                "Unexpected live response attachment order",
            )


async def run_live_suite(
    *,
    token: str,
    user_id: int,
    mongo_uri: str,
) -> dict[str, Any]:
    live_db = f"max_sender_e2e_live_{int(time.time())}"
    mongo = MongoProbe(mongo_uri, live_db)
    results: dict[str, Any] = {"mongo_db": live_db, "scenarios": {}}

    print_step(f"Running live MAX suite against Mongo DB {live_db}")

    async with ClientSession() as session:
        with tempfile.TemporaryDirectory(prefix="max_sender_e2e_assets_") as temp_dir:
            asset_dir = Path(temp_dir)
            image_path = await download_first_available(
                session, IMAGE_URLS, ".png", asset_dir
            )
            video_path = await download_first_available(
                session, VIDEO_URLS, ".mp4", asset_dir
            )
            image_token = await upload_media(
                session,
                token=token,
                upload_type="image",
                file_path=image_path,
            )
            video_token = await upload_media(
                session,
                token=token,
                upload_type="video",
                file_path=video_path,
            )
            print_step(
                "Sleeping briefly after media upload to reduce attachment-not-ready flakiness"
            )
            await asyncio.sleep(5.0)

    prefix = f"E2E-{int(time.time())}"

    html_text = f"{prefix}-html <b>ok</b>"
    html_sender = MaxSender(
        token=token,
        use_mongo=True,
        mongo_uri=mongo_uri,
        mongo_db=live_db,
        parse_mode="HTML",
    )
    html_outcome, html_docs, _ = await run_sender_and_capture(
        html_sender,
        mongo,
        recipient_ids=[user_id],
        text=html_text,
    )
    ensure_equal(
        (html_outcome.delivered, html_outcome.not_delivered),
        (1, 0),
        "HTML text send failed",
    )
    verify_live_success_documents(
        html_docs,
        recipient_id=user_id,
        expected_count=1,
        expected_text=html_text,
        expected_format="html",
        expected_attachment_types=[],
        expected_inline_buttons=0,
        disable_link_preview=False,
        response_text_contains=[f"{prefix}-html", "ok"],
    )
    results["scenarios"]["html_text"] = html_outcome.__dict__

    markdown_text = f"{prefix}-markdown *ok* https://example.com"
    markdown_sender = MaxSender(
        token=token,
        use_mongo=True,
        mongo_uri=mongo_uri,
        mongo_db=live_db,
        parse_mode="Markdown",
    )
    markdown_outcome, markdown_docs, _ = await run_sender_and_capture(
        markdown_sender,
        mongo,
        recipient_ids=[user_id],
        text=markdown_text,
        disable_web_page_preview=True,
    )
    ensure_equal(
        (markdown_outcome.delivered, markdown_outcome.not_delivered),
        (1, 0),
        "Markdown text send failed",
    )
    verify_live_success_documents(
        markdown_docs,
        recipient_id=user_id,
        expected_count=1,
        expected_text=markdown_text,
        expected_format="markdown",
        expected_attachment_types=[],
        expected_inline_buttons=0,
        disable_link_preview=True,
        response_text_contains=[f"{prefix}-markdown", "ok", "https://example.com"],
    )
    results["scenarios"]["markdown_text"] = markdown_outcome.__dict__

    keyboard_text = f"{prefix}-keyboard"
    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "Docs", "url": "https://dev.max.ru/docs-api"},
                {"text": "Ping", "callback_data": "ping"},
            ]
        ]
    }
    keyboard_sender = MaxSender(
        token=token,
        use_mongo=True,
        mongo_uri=mongo_uri,
        mongo_db=live_db,
    )
    keyboard_outcome, keyboard_docs, _ = await run_sender_and_capture(
        keyboard_sender,
        mongo,
        recipient_ids=[user_id],
        text=keyboard_text,
        reply_markup=reply_markup,
    )
    ensure_equal(
        (keyboard_outcome.delivered, keyboard_outcome.not_delivered),
        (1, 0),
        "Inline keyboard send failed",
    )
    verify_live_success_documents(
        keyboard_docs,
        recipient_id=user_id,
        expected_count=1,
        expected_text=keyboard_text,
        expected_format="html",
        expected_attachment_types=["inline_keyboard"],
        expected_inline_buttons=2,
        disable_link_preview=False,
        expected_response_text=keyboard_text,
    )
    results["scenarios"]["inline_keyboard"] = keyboard_outcome.__dict__

    photo_text = f"{prefix}-photo"
    photo_sender = MaxSender(
        token=token,
        use_mongo=True,
        mongo_uri=mongo_uri,
        mongo_db=live_db,
    )
    photo_outcome, photo_docs, _ = await run_sender_and_capture(
        photo_sender,
        mongo,
        recipient_ids=[user_id],
        text=photo_text,
        media_items=[Photo(image_token)],
    )
    ensure_equal(
        (photo_outcome.delivered, photo_outcome.not_delivered),
        (1, 0),
        "Photo send failed",
    )
    verify_live_success_documents(
        photo_docs,
        recipient_id=user_id,
        expected_count=1,
        expected_text=photo_text,
        expected_format="html",
        expected_attachment_types=["image"],
        expected_inline_buttons=0,
        disable_link_preview=False,
        expected_response_text=photo_text,
    )
    results["scenarios"]["photo"] = photo_outcome.__dict__

    video_text = f"{prefix}-video"
    video_sender = MaxSender(
        token=token,
        use_mongo=True,
        mongo_uri=mongo_uri,
        mongo_db=live_db,
    )
    video_outcome, video_docs, _ = await run_sender_and_capture(
        video_sender,
        mongo,
        recipient_ids=[user_id],
        text=video_text,
        media_items=[Video(video_token)],
    )
    ensure_equal(
        (video_outcome.delivered, video_outcome.not_delivered),
        (1, 0),
        "Video send failed",
    )
    verify_live_success_documents(
        video_docs,
        recipient_id=user_id,
        expected_count=1,
        expected_text=video_text,
        expected_format="html",
        expected_attachment_types=["video"],
        expected_inline_buttons=0,
        disable_link_preview=False,
        expected_response_text=video_text,
    )
    results["scenarios"]["video"] = video_outcome.__dict__

    mixed_text = f"{prefix}-mixed"
    mixed_sender = MaxSender(
        token=token,
        use_mongo=True,
        mongo_uri=mongo_uri,
        mongo_db=live_db,
    )
    mixed_outcome, mixed_docs, _ = await run_sender_and_capture(
        mixed_sender,
        mongo,
        recipient_ids=[user_id],
        text=mixed_text,
        media_items=[Photo(image_token), Video(video_token), Photo(image_token)],
    )
    ensure_equal(
        (mixed_outcome.delivered, mixed_outcome.not_delivered),
        (1, 0),
        "Mixed media send failed",
    )
    verify_live_success_documents(
        mixed_docs,
        recipient_id=user_id,
        expected_count=1,
        expected_text=mixed_text,
        expected_format="html",
        expected_attachment_types=["image", "video", "image"],
        expected_inline_buttons=0,
        disable_link_preview=False,
        expected_response_text=mixed_text,
    )
    results["scenarios"]["mixed_media"] = mixed_outcome.__dict__

    batch_text = f"{prefix}-batch60"
    batch_sender = MaxSender(
        token=token,
        batch_size=180,
        delay_between_batches=0.0,
        use_mongo=True,
        mongo_uri=mongo_uri,
        mongo_db=live_db,
    )
    batch_outcome, batch_docs, batch_elapsed = await run_sender_and_capture(
        batch_sender,
        mongo,
        recipient_ids=[user_id] * 60,
        text=batch_text,
    )
    ensure_equal(
        (batch_outcome.delivered, batch_outcome.not_delivered),
        (60, 0),
        "Batch send failed",
    )
    ensure_equal(
        len(batch_docs), 60, "Batch scenario should create exactly 60 Mongo documents"
    )
    ensure(
        batch_elapsed >= 2.1,
        f"Batch pacing check failed; expected elapsed >= 2.1s, got {batch_elapsed:.3f}s",
    )
    verify_live_success_documents(
        batch_docs,
        recipient_id=user_id,
        expected_count=60,
        expected_text=batch_text,
        expected_format="html",
        expected_attachment_types=[],
        expected_inline_buttons=0,
        disable_link_preview=False,
        expected_response_text=batch_text,
    )
    results["scenarios"]["batch60"] = {
        **batch_outcome.__dict__,
        "elapsed_seconds": batch_elapsed,
    }

    invalid_text = f"{prefix}-invalid-photo"
    invalid_sender = MaxSender(
        token=token,
        use_mongo=True,
        mongo_uri=mongo_uri,
        mongo_db=live_db,
    )
    invalid_outcome, invalid_docs, _ = await run_sender_and_capture(
        invalid_sender,
        mongo,
        recipient_ids=[user_id],
        text=invalid_text,
        media_items=[Photo("definitely-invalid-max-token")],
    )
    ensure_equal(
        (invalid_outcome.delivered, invalid_outcome.not_delivered),
        (0, 1),
        "Permanent live failure scenario should not deliver",
    )
    ensure_equal(
        len(invalid_docs), 1, "Permanent live failure should log exactly one attempt"
    )
    invalid_doc = invalid_docs[0]
    verify_common_document_fields(
        invalid_doc,
        recipient_id=user_id,
        recipient_type="user",
    )
    ensure_equal(
        invalid_doc["delivered"], False, "Permanent live failure must not be delivered"
    )
    ensure_equal(
        invalid_doc["will_retry"], False, "Permanent live failure must not retry"
    )
    ensure_equal(
        invalid_doc["temporary_error"],
        False,
        "Permanent live failure must not be temporary",
    )
    ensure_equal(invalid_doc["attempt"], 1, "Permanent live failure attempt mismatch")
    ensure(
        invalid_doc["http_status"] in {400, 404},
        f"Unexpected permanent live failure status {invalid_doc['http_status']!r}",
    )
    response = invalid_doc["response"]
    ensure(isinstance(response, dict), "Permanent live failure response must be a dict")
    error_code = response.get("code") or invalid_doc["error_code"]
    ensure(error_code is not None, "Permanent live failure must capture an error code")
    verify_payload_summary(
        invalid_doc,
        has_text=True,
        text_length=len(invalid_text),
        format_value="html",
        attachment_types=["image"],
        inline_button_count=0,
        disable_link_preview=False,
    )
    results["scenarios"]["permanent_live_failure"] = invalid_outcome.__dict__

    mongo.close()
    return results


class RetryStub:
    def __init__(self) -> None:
        self.attempts: defaultdict[str, int] = defaultdict(int)
        self._app = web.Application()
        self._app.router.add_post("/scenario/{name}", self.handle)
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self.port: int | None = None

    async def __aenter__(self) -> "RetryStub":
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        sockets = self._site._server.sockets  # type: ignore[attr-defined]
        ensure(sockets, "Stub server did not expose sockets")
        self.port = sockets[0].getsockname()[1]
        print_step(f"Started retry stub on 127.0.0.1:{self.port}")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    async def handle(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        self.attempts[name] += 1
        attempt = self.attempts[name]

        if name == "retry_429":
            if attempt == 1:
                return web.json_response(
                    {"code": "too.many.requests", "retry_after": 0.01},
                    status=429,
                )
            return web.json_response({"message": {"id": f"{name}-{attempt}"}})

        if name == "retry_503":
            if attempt == 1:
                return web.json_response({"message": "unavailable"}, status=503)
            return web.json_response({"message": {"id": f"{name}-{attempt}"}})

        if name == "attachment_not_ready":
            if attempt == 1:
                return web.json_response(
                    {
                        "code": "attachment.not.ready",
                        "message": "Key: errors.process.attachment.file.not.processed",
                    },
                    status=400,
                )
            return web.json_response({"message": {"id": f"{name}-{attempt}"}})

        if name == "permanent_400":
            return web.json_response(
                {"code": "bad.request", "message": "bad"}, status=400
            )

        return web.json_response({"message": {"id": f"default-{attempt}"}})


def unused_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def verify_stub_retry_documents(
    documents: list[dict[str, Any]],
    *,
    recipient_id: int,
    expected_attempts: list[int],
    expected_http_statuses: list[int | None],
    expected_delivered: list[bool],
    expected_will_retry: list[bool],
    expected_temporary_error: list[bool],
    expected_text: str,
    expected_attachment_types: list[str],
    expect_exception: bool = False,
) -> None:
    ensure_equal(
        len(documents), len(expected_attempts), "Unexpected stub document count"
    )
    for index, document in enumerate(documents):
        verify_common_document_fields(
            document,
            recipient_id=recipient_id,
            recipient_type="user",
        )
        ensure_equal(
            document["attempt"], expected_attempts[index], "Unexpected stub attempt"
        )
        ensure_equal(
            document["http_status"],
            expected_http_statuses[index],
            "Unexpected stub http_status",
        )
        ensure_equal(
            document["delivered"],
            expected_delivered[index],
            "Unexpected stub delivered flag",
        )
        ensure_equal(
            document["will_retry"],
            expected_will_retry[index],
            "Unexpected stub will_retry flag",
        )
        ensure_equal(
            document["temporary_error"],
            expected_temporary_error[index],
            "Unexpected stub temporary_error flag",
        )
        verify_payload_summary(
            document,
            has_text=True,
            text_length=len(expected_text),
            format_value="html",
            attachment_types=expected_attachment_types,
            inline_button_count=0,
            disable_link_preview=False,
        )
        if expect_exception:
            ensure(document["exception"], "Expected stub exception to be captured")
        else:
            ensure_equal(document["exception"], None, "Unexpected stub exception")


async def run_stub_suite(
    *,
    mongo_uri: str,
) -> dict[str, Any]:
    stub_db = f"max_sender_e2e_stub_{int(time.time())}"
    mongo = MongoProbe(mongo_uri, stub_db)
    results: dict[str, Any] = {"mongo_db": stub_db, "scenarios": {}}

    print_step(f"Running deterministic retry suite against Mongo DB {stub_db}")

    async with RetryStub() as stub:
        recipient_id = 4600731
        base_url = f"http://127.0.0.1:{stub.port}/scenario"

        retry_429_sender = MaxSender(
            token="stub-token",
            use_mongo=True,
            mongo_uri=mongo_uri,
            mongo_db=stub_db,
        )
        retry_429_sender._url = f"{base_url}/retry_429"
        outcome_429, docs_429, _ = await run_sender_and_capture(
            retry_429_sender,
            mongo,
            recipient_ids=[recipient_id],
            text="stub-429",
        )
        ensure_equal(
            (outcome_429.delivered, outcome_429.not_delivered),
            (1, 0),
            "429 stub send failed",
        )
        verify_stub_retry_documents(
            docs_429,
            recipient_id=recipient_id,
            expected_attempts=[1, 2],
            expected_http_statuses=[429, 200],
            expected_delivered=[False, True],
            expected_will_retry=[True, False],
            expected_temporary_error=[True, False],
            expected_text="stub-429",
            expected_attachment_types=[],
        )
        ensure_equal(
            docs_429[0]["retry_after"],
            0.01,
            "429 retry_after should come from response body",
        )
        results["scenarios"]["retry_429"] = outcome_429.__dict__

        retry_503_sender = MaxSender(
            token="stub-token",
            use_mongo=True,
            mongo_uri=mongo_uri,
            mongo_db=stub_db,
        )
        retry_503_sender._url = f"{base_url}/retry_503"
        outcome_503, docs_503, _ = await run_sender_and_capture(
            retry_503_sender,
            mongo,
            recipient_ids=[recipient_id],
            text="stub-503",
        )
        ensure_equal(
            (outcome_503.delivered, outcome_503.not_delivered),
            (1, 0),
            "503 stub send failed",
        )
        verify_stub_retry_documents(
            docs_503,
            recipient_id=recipient_id,
            expected_attempts=[1, 2],
            expected_http_statuses=[503, 200],
            expected_delivered=[False, True],
            expected_will_retry=[True, False],
            expected_temporary_error=[True, False],
            expected_text="stub-503",
            expected_attachment_types=[],
        )
        ensure_equal(
            docs_503[0]["retry_after"], None, "503 retry_after should be empty"
        )
        results["scenarios"]["retry_503"] = outcome_503.__dict__

        attachment_sender = MaxSender(
            token="stub-token",
            use_mongo=True,
            mongo_uri=mongo_uri,
            mongo_db=stub_db,
        )
        attachment_sender._url = f"{base_url}/attachment_not_ready"
        outcome_attachment, docs_attachment, _ = await run_sender_and_capture(
            attachment_sender,
            mongo,
            recipient_ids=[recipient_id],
            text="stub-attachment",
            media_items=[Photo("ready-later-token")],
        )
        ensure_equal(
            (outcome_attachment.delivered, outcome_attachment.not_delivered),
            (1, 0),
            "attachment.not.ready stub send failed",
        )
        verify_stub_retry_documents(
            docs_attachment,
            recipient_id=recipient_id,
            expected_attempts=[1, 2],
            expected_http_statuses=[400, 200],
            expected_delivered=[False, True],
            expected_will_retry=[True, False],
            expected_temporary_error=[True, False],
            expected_text="stub-attachment",
            expected_attachment_types=["image"],
        )
        ensure_equal(
            docs_attachment[0]["error_code"],
            "attachment.not.ready",
            "attachment.not.ready error_code mismatch",
        )
        results["scenarios"]["attachment_not_ready"] = outcome_attachment.__dict__

        network_sender = MaxSender(
            token="stub-token",
            use_mongo=True,
            mongo_uri=mongo_uri,
            mongo_db=stub_db,
        )
        network_sender._url = (
            f"http://127.0.0.1:{unused_local_port()}/scenario/network_error"
        )
        outcome_network, docs_network, _ = await run_sender_and_capture(
            network_sender,
            mongo,
            recipient_ids=[recipient_id],
            text="stub-network",
        )
        ensure_equal(
            (outcome_network.delivered, outcome_network.not_delivered),
            (0, 1),
            "Network error stub send should exhaust retries",
        )
        verify_stub_retry_documents(
            docs_network,
            recipient_id=recipient_id,
            expected_attempts=[1, 2, 3, 4],
            expected_http_statuses=[None, None, None, None],
            expected_delivered=[False, False, False, False],
            expected_will_retry=[True, True, True, False],
            expected_temporary_error=[True, True, True, True],
            expected_text="stub-network",
            expected_attachment_types=[],
            expect_exception=True,
        )
        results["scenarios"]["network_error"] = outcome_network.__dict__

        permanent_sender = MaxSender(
            token="stub-token",
            use_mongo=True,
            mongo_uri=mongo_uri,
            mongo_db=stub_db,
        )
        permanent_sender._url = f"{base_url}/permanent_400"
        outcome_permanent, docs_permanent, _ = await run_sender_and_capture(
            permanent_sender,
            mongo,
            recipient_ids=[recipient_id],
            text="stub-400",
        )
        ensure_equal(
            (outcome_permanent.delivered, outcome_permanent.not_delivered),
            (0, 1),
            "Permanent 400 stub send should fail without retry",
        )
        verify_stub_retry_documents(
            docs_permanent,
            recipient_id=recipient_id,
            expected_attempts=[1],
            expected_http_statuses=[400],
            expected_delivered=[False],
            expected_will_retry=[False],
            expected_temporary_error=[False],
            expected_text="stub-400",
            expected_attachment_types=[],
        )
        ensure_equal(
            docs_permanent[0]["error_code"],
            "bad.request",
            "Permanent 400 error_code mismatch",
        )
        results["scenarios"]["permanent_400"] = outcome_permanent.__dict__

    mongo.close()
    return results


async def async_main(args: argparse.Namespace) -> int:
    summary: dict[str, Any] = {
        "mode": args.mode,
        "package_path": str(Path(max_sender.__file__).resolve()),
    }

    await assert_separate_install()
    await run_validation_checks()
    summary["validation"] = "ok"

    if args.mode in {"all", "live"}:
        token = os.environ.get(args.token_env)
        ensure(
            token, f"Environment variable {args.token_env} is required for live mode"
        )
        summary["live"] = await run_live_suite(
            token=token,
            user_id=args.user_id,
            mongo_uri=args.mongo_uri,
        )

    if args.mode in {"all", "stub"}:
        summary["stub"] = await run_stub_suite(mongo_uri=args.mongo_uri)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="E2E verification harness for max_sender"
    )
    parser.add_argument(
        "--mode",
        choices=("all", "live", "stub"),
        default="all",
        help="Which suites to execute",
    )
    parser.add_argument(
        "--mongo-uri",
        default="mongodb://localhost:27017",
        help="MongoDB connection string for log verification",
    )
    parser.add_argument(
        "--user-id",
        type=int,
        default=4600731,
        help="MAX recipient user_id for live sends",
    )
    parser.add_argument(
        "--token-env",
        default="MAX_E2E_TOKEN",
        help="Environment variable name that stores the MAX bot token",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(async_main(args))
    except VerificationError as exc:
        print(f"[e2e] FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
