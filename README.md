# max_sender: Asynchronous MAX Broadcast Sender

`max_sender` is an asynchronous Python package for bulk delivery through the MAX bot API. It keeps the original workflow intact where it matters: one async sender instance, batched delivery, retry handling for temporary failures, optional MongoDB logging, and support for text, media, and inline keyboards.

## Features

- Async batch delivery over `aiohttp`
- One `run(...)` call for text, media, or media plus inline keyboard
- Explicit `recipient_type="user"` or `recipient_type="chat"`
- Internal pacing guard that keeps delivery on the MAX-safe profile even if higher batch values are requested
- Automatic retry for temporary MAX failures:
  - HTTP `429`
  - HTTP `503`
  - network exceptions
  - MAX error code `attachment.not.ready`
- Sender-level pause and retry queue: temporary failures pause the whole broadcast, retry the affected messages, and only then continue with later batches
- When MAX answers `429` without a `Retry-After`, the sender falls back to a longer rate-limit cooldown instead of the generic short retry interval
- Optional MongoDB logging for every send attempt
- MAX-compatible `HTML` and `Markdown` formatting

## Installation

```bash
pip install max_sender
uv add max_sender
```

Python 3.10+ is required.

## MAX API mapping

- Text and media are sent through `POST /messages`
- Recipients are addressed with either `user_id` or `chat_id`
- Media helpers `Photo(...)` and `Video(...)` expect ready-to-use MAX media tokens
- `reply_markup.inline_keyboard` is converted into a MAX `inline_keyboard` attachment

## Quick Start

### Text broadcast

```python
import asyncio

from max_sender import MaxSender


async def main() -> None:
    sender = MaxSender(
        token="YOUR_MAX_BOT_TOKEN",
        batch_size=25,
        delay_between_batches=1.1,
        use_mongo=False,
        parse_mode="HTML",
    )

    delivered, not_delivered = await sender.run(
        [4600731, 4600732],
        text="Hello from <b>MAX</b>.",
        recipient_type="user",
    )

    print(f"Delivered: {delivered}, failed: {not_delivered}")


asyncio.run(main())
```

### Text plus inline keyboard

```python
import asyncio

from max_sender import MaxSender


async def main() -> None:
    sender = MaxSender(token="YOUR_MAX_BOT_TOKEN", use_mongo=False)

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "Open docs", "url": "https://dev.max.ru/docs-api"},
                {"text": "Ping", "callback_data": "ping"},
            ]
        ]
    }

    delivered, not_delivered = await sender.run(
        [4600731],
        text="Choose an action.",
        reply_markup=reply_markup,
        recipient_type="user",
    )

    print(f"Delivered: {delivered}, failed: {not_delivered}")


asyncio.run(main())
```

### Media broadcast with ready MAX tokens

```python
import asyncio

from max_sender import MaxSender, Photo, Video


async def main() -> None:
    sender = MaxSender(token="YOUR_MAX_BOT_TOKEN", use_mongo=False)

    delivered, not_delivered = await sender.run(
        [4600731],
        text="Media payload in original order.",
        media_items=[
            Photo("MAX_IMAGE_TOKEN"),
            Video("MAX_VIDEO_TOKEN"),
            Photo("MAX_IMAGE_TOKEN_2"),
        ],
        recipient_type="user",
    )

    print(f"Delivered: {delivered}, failed: {not_delivered}")


asyncio.run(main())
```

## MongoDB logging

When `use_mongo=True`, each attempt is stored in a collection named by the Moscow-time launch timestamp. Each document includes:

- `recipient_id`
- `recipient_type`
- `attempt`
- `delivered`
- `http_status`
- raw MAX response body
- exception details, if any
- compact payload summary

## Notes

- `parse_mode` supports only `HTML` and `Markdown`
- `disable_web_page_preview` maps to MAX `disable_link_preview`
- `Photo(...)` maps to MAX attachment type `image`
- Delivery uses a conservative internal profile of up to 25 concurrent sends with at least 1.1 seconds between batch starts
- Temporary delivery errors are retried via a shared queue, so `429` pauses the sender instead of immediately dropping later messages
- Rate-limit retries use a stronger fallback cooldown than generic temporary errors because MAX often omits `Retry-After` on `429`
- Local file upload is intentionally out of scope for v1; upload files separately through `POST /uploads` and reuse the returned tokens

## MAX references

- [MAX API overview](https://dev.max.ru/docs-api)
- [Send message](https://dev.max.ru/docs-api/methods/POST/messages)
- [Upload files](https://dev.max.ru/docs-api/methods/POST/uploads)
- [Get updates](https://dev.max.ru/docs-api/methods/GET/updates)
