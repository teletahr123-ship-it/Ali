"""
بوت الجلسة (حساب Pyrogram) - يعمل على استضافة بايثون الخاصة بك (ليس على كلاود فلاير).

وظيفته:
  1) يراقب المجموعة المحددة، وعندما يكتب بوت الاستلام "طلب بحث جديد" يبحث في القنوات
     ثم يرد على رسالة البوت بكليشة فيها الكتب (سطر تحت سطر) مع حجم كل كتاب.
  2) وعندما يرد بوت الاستلام على الكليشة برقم كتاب، يرسل الحساب هذا الكتاب إلى
     المجموعة كرد على رسالة البوت (بدون وصف)، فيستلمه البوت ويحوله للمستخدم في الخاص.

المتطلبات (requirements.txt):
    pyrogram
    tgcrypto
    pymongo

المتغيرات (Secrets / Environment):
    API_ID, API_HASH, SESSION_STR   جلسة Pyrogram (String Session) للحساب
    GROUP_ID                        معرف المجموعة التي يعمل فيها
    RECEIVER_BOT_ID                 ايدي بوت الاستلام (الرقم الذي قبل : في توكن البوت)
    MONGODB_URI                     رابط MongoDB
    MONGODB_DB                      (اختياري) اسم القاعدة، الافتراضي book_bot
    CHANNELS                        القنوات مفصولة بفاصلة: @ch1,https://t.me/ch2,ch3
"""
import asyncio
import logging
import os
import re
import unicodedata
from datetime import datetime, timezone
from typing import Optional

from pymongo import MongoClient
from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    ChannelInvalid,
    FloodWait,
    PeerIdInvalid,
    UsernameInvalid,
    UsernameNotOccupied,
)
from pyrogram.handlers import MessageHandler

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ==============================================================
#  إعدادات تتحكم بها (غيّر الأرقام كما تشاء)
# ==============================================================
BOOKS_PER_SEARCH = 100          # عدد الكتب التي يرسلها الحساب لكل بحث
ALLOW_MULTIPLE_MESSAGES = True  # True: إذا لم تتسع الكتب في رسالة واحدة يقسمها لعدة كليشات
                                # False: كليشة واحدة فقط، ويقصّر الأسماء (أو يحذف الأخيرة) لتتسع
NAME_MAX_CHARS = 60             # أقصى عدد أحرف لاسم الكتاب في السطر
MIN_NAME_CHARS = 24             # أقل طول للاسم عند التقصير (لا تجعله أقل من 24)
SEARCH_LIMIT = 500              # عدد الرسائل التي يفحصها في كل قناة
SESSION_TTL_DAYS = 3            # مدة حفظ نتائج البحث (لتعمل أرقام الكتب)
PART_DELAY = 0.7                # ثواني الانتظار بين أجزاء النتيجة

TEXT_LIMIT = 4000               # حد رسالة تيليجرام (4096) مع هامش أمان
CHUNK_LIMIT = 3800              # حد الجزء الواحد عند التقسيم (يترك مكانا للترويسة)

# ==============================================================
#  المتغيرات من البيئة
# ==============================================================
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STR = os.environ["SESSION_STR"]
GROUP_ID = int(os.environ["GROUP_ID"])
RECEIVER_BOT_ID = int(os.environ["RECEIVER_BOT_ID"])
MONGODB_URI = os.environ["MONGODB_URI"].strip()
MONGODB_DB = os.environ.get("MONGODB_DB", "book_bot").strip()
_ENV_CHANNELS = os.environ.get("CHANNELS", "")

REQUEST_PREFIX = "⌯ طلب بحث جديد"

# كلمات وصفية لا تدخل في اسم الكتاب نفسه عند بداية البحث.
_SEARCH_PREFIX_WORDS = {"مكتبة", "المكتبة"}

# ==============================================================
#  MongoDB
# ==============================================================
_mongo_client = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=10000,
    connectTimeoutMS=10000,
)
_mongo_db = _mongo_client[MONGODB_DB]
_settings = _mongo_db["settings"]
_sessions = _mongo_db["book_sessions"]
_data_cache: Optional[dict] = None


def init_mongodb() -> None:
    _mongo_client.admin.command("ping")
    _sessions.create_index("created_at", expireAfterSeconds=SESSION_TTL_DAYS * 86400)
    _sessions.create_index("msg_ids")
    logger.info("MongoDB connected")


def load_data() -> dict:
    global _data_cache
    if _data_cache is None:
        doc = _settings.find_one({"_id": "app"}) or {}
        _data_cache = {
            "channels": doc.get("channels", []),
            "channel_ids": doc.get("channel_ids", {}),
        }
    return _data_cache


def save_data(data: dict) -> None:
    global _data_cache
    _data_cache = data
    _settings.replace_one(
        {"_id": "app"},
        {
            "_id": "app",
            "channels": data.get("channels", []),
            "channel_ids": data.get("channel_ids", {}),
        },
        upsert=True,
    )


def save_session(msg_ids: list, query: str, books: list) -> None:
    _sessions.insert_one({
        "msg_ids": msg_ids,
        "query": query,
        "books": books,
        "created_at": datetime.now(timezone.utc),
    })


def load_session(msg_id: int) -> Optional[dict]:
    return _sessions.find_one({"msg_ids": msg_id})


# ==============================================================
#  القنوات
# ==============================================================
def normalize_channel(text: str) -> str:
    text = text.strip()
    if text.startswith("https://t.me/"):
        text = "@" + text.replace("https://t.me/", "").split("/")[0].rstrip("/")
    elif text.startswith("t.me/"):
        text = "@" + text.replace("t.me/", "").split("/")[0].rstrip("/")
    elif not text.startswith("@"):
        text = "@" + text
    return text


def parse_env_channels() -> list:
    result = []
    for part in _ENV_CHANNELS.split(","):
        if not part.strip():
            continue
        ch = normalize_channel(part)
        if ch not in result:
            result.append(ch)
    return result


def sync_channels_from_env() -> dict:
    """يجعل متغير CHANNELS المصدر الوحيد للقنوات."""
    data = load_data()
    channels = parse_env_channels()
    old_ids = data.get("channel_ids", {})
    data["channels"] = channels
    data["channel_ids"] = {c: old_ids[c] for c in channels if c in old_ids}
    save_data(data)
    logger.info("Channels: " + (", ".join(channels) if channels else "(none)"))
    return data


# ==============================================================
#  Pyrogram
# ==============================================================
app: Optional[Client] = None
_search_lock: Optional[asyncio.Lock] = None
_tasks: set = set()


def spawn(coro) -> None:
    """يشغّل المهمة في الخلفية دون أن يحجز عامل Pyrogram."""
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def flood_retry(fn, *args, **kwargs):
    """يعيد المحاولة عند FloodWait (حتى 3 مرات)."""
    for attempt in range(3):
        try:
            return await fn(*args, **kwargs)
        except FloodWait as e:
            wait = min(int(e.value) + 1, 90)
            logger.warning(f"FloodWait {wait}s")
            if attempt == 2:
                raise
            await asyncio.sleep(wait)


def is_pdf(msg) -> bool:
    doc = msg.document
    if not doc:
        return False
    if doc.mime_type == "application/pdf":
        return True
    return bool(doc.file_name and doc.file_name.lower().endswith(".pdf"))


def human_size(n: Optional[int]) -> str:
    n = n or 0
    if n >= 1024 ** 3:
        return f"{n / 1024 ** 3:.1f}GB"
    if n >= 1024 ** 2:
        return f"{n / 1024 ** 2:.1f}MB"
    return f"{max(1, round(n / 1024))}KB"


def clean_name(name: str) -> str:
    """ينظف اسم الكتاب ليبقى سطرا واحدا بدون رموز تكسر تنسيق الكليشة."""
    name = re.sub(r"\.pdf$", "", name.strip(), flags=re.I)
    name = name.replace("_", " ").replace("‹", " ").replace("›", " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name or "كتاب"


async def resolve_channel(username: str) -> Optional[int]:
    data = load_data()
    ids = data.setdefault("channel_ids", {})
    if username in ids:
        return ids[username]
    try:
        chat = await asyncio.wait_for(app.get_chat(username), timeout=20)
        ids[username] = chat.id
        save_data(data)
        logger.info(f"Resolved {username} -> {chat.id}")
        return chat.id
    except asyncio.TimeoutError:
        logger.warning(f"Timeout resolving {username}")
    except (PeerIdInvalid, ChannelInvalid, UsernameInvalid):
        logger.warning(f"Cannot resolve {username}")
    except FloodWait as e:
        logger.warning(f"FloodWait {e.value}s resolving {username}")
    except Exception as e:
        logger.warning(f"Error resolving {username}: {e}")
    return None


async def _search_channel(ch: str, query: str, ids: dict) -> list:
    peer = ids.get(ch) or ch
    results = []
    try:
        async for msg in app.search_messages(peer, query=query, limit=SEARCH_LIMIT):
            if not is_pdf(msg):
                continue
            name = (msg.document.file_name or "").strip()
            if not name and msg.caption:
                name = msg.caption.split("\n")[0].strip()
            results.append({
                "name": clean_name(name or "ملف PDF"),
                "size": human_size(msg.document.file_size),
                "chat": ch,
                "msg_id": msg.id,
            })
    except (PeerIdInvalid, ChannelInvalid, UsernameInvalid, UsernameNotOccupied):
        logger.warning(f"Channel not found or inaccessible: {ch}")
        data = load_data()
        data.get("channel_ids", {}).pop(ch, None)
        save_data(data)
    except FloodWait as e:
        logger.warning(f"FloodWait {e.value}s in {ch} - skipping")
    except Exception as e:
        logger.error(f"Search error ({ch}): {e}")
    return results


async def search_libraries(query: str, channels: list) -> list:
    # جلسة واحدة = بحث واحد في كل مرة، حتى لا يفرض تيليجرام انتظارا (غالبا 31 ثانية).
    async with _search_lock:
        ids = load_data().get("channel_ids", {})
        per_channel = await asyncio.gather(*[_search_channel(c, query, ids) for c in channels])
        return [r for chunk in per_channel for r in chunk]


def dedupe(books: list) -> list:
    seen = set()
    out = []
    for b in books:
        key = (b["name"].lower(), b["size"])
        if key in seen:
            continue
        seen.add(key)
        out.append(b)
    return out


# ==============================================================
#  تنظيف كلمة البحث
# ==============================================================
def _canonical(word: str) -> str:
    return word.casefold().translate(str.maketrans({
        "أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا",
        "ى": "ي", "ة": "ه", "ؤ": "و", "ئ": "ي",
    }))


_CANON_PREFIX = {_canonical(w) for w in _SEARCH_PREFIX_WORDS}


def normalize_search_query(text: str) -> str:
    cleaned = []
    for char in text:
        cat = unicodedata.category(char)
        if char == "ـ" or cat.startswith("M"):
            continue
        cleaned.append(" " if cat.startswith(("P", "S", "C")) else char)
    words = "".join(cleaned).split()
    while words and _canonical(words[0]) in _CANON_PREFIX:
        words.pop(0)
    return " ".join(words)


# ==============================================================
#  بناء الكليشة
#  شكل السطر:  12. اسم الكتاب ‹3.4MB›
#  (بوت الاستلام يقرأ هذا الشكل، فلا تغيّره)
# ==============================================================
def make_header(query: str, total: int, part: int, parts: int) -> str:
    return (
        f"⌯ نتائج البحث عن: {query}\n"
        f"⌯ عدد الكتب: {total}\n"
        f"⌯ الجزء: {part}/{parts}\n\n"
    )


def fmt_line(i: int, book: dict, name_max: int) -> str:
    name = book["name"]
    if len(name) > name_max:
        name = name[: name_max - 1].rstrip() + "…"
    return f"{i}. {name} ‹{book['size']}›"


def build_messages(query: str, books: list):
    """يرجع (قائمة نصوص الرسائل، قائمة الكتب التي دخلت فعلا)."""
    total = len(books)
    name_max = max(NAME_MAX_CHARS, MIN_NAME_CHARS)

    if ALLOW_MULTIPLE_MESSAGES:
        chunks, cur, size = [], [], 0
        for i, b in enumerate(books, 1):
            line = fmt_line(i, b, name_max)
            if cur and size + len(line) + 1 > CHUNK_LIMIT:
                chunks.append(cur)
                cur, size = [], 0
            cur.append(line)
            size += len(line) + 1
        if cur:
            chunks.append(cur)
        parts = [
            make_header(query, total, idx, len(chunks)) + "\n".join(chunk)
            for idx, chunk in enumerate(chunks, 1)
        ]
        return parts, books

    # كليشة واحدة فقط: نقصّر الأسماء تدريجيا حتى تتسع
    for nm in range(name_max, MIN_NAME_CHARS - 1, -4):
        lines = [fmt_line(i, b, nm) for i, b in enumerate(books, 1)]
        text = make_header(query, total, 1, 1) + "\n".join(lines)
        if len(text) <= TEXT_LIMIT:
            return [text], books
    # ما زالت أطول من الحد: نحذف الكتب الأخيرة
    kept, size = [], len(make_header(query, total, 1, 1))
    for i, b in enumerate(books, 1):
        line = fmt_line(i, b, MIN_NAME_CHARS)
        if size + len(line) + 1 > TEXT_LIMIT:
            break
        kept.append(line)
        size += len(line) + 1
    books = books[: len(kept)]
    return [make_header(query, len(books), 1, 1) + "\n".join(kept)], books


# ==============================================================
#  المعالجة
# ==============================================================
async def reply(message, text: str):
    return await flood_retry(message.reply_text, text, quote=True)


async def handle_search_request(message, text: str) -> None:
    m = re.search(r"^⌯ الكلمة:\s*(.+?)\s*$", text, re.M)
    query = normalize_search_query(m.group(1)) if m else ""
    logger.info(f"Search request: {query!r}")

    books = []
    channels = load_data().get("channels", [])
    if query and channels:
        books = await search_libraries(query, channels)
        words = query.split()
        if not books and len(words) > 2:
            books = await search_libraries(" ".join(words[:2]), channels)
    books = dedupe(books)[:BOOKS_PER_SEARCH]

    if not books:
        await reply(message, make_header(query or "-", 0, 1, 1) + "⌯ لا توجد نتائج")
        return

    parts, books = build_messages(query, books)
    msg_ids = []
    for idx, part_text in enumerate(parts):
        sent = await reply(message, part_text)
        msg_ids.append(sent.id)
        if idx < len(parts) - 1:
            await asyncio.sleep(PART_DELAY)
    await asyncio.to_thread(save_session, msg_ids, query, books)
    logger.info(f"Sent {len(books)} books in {len(parts)} message(s)")


async def handle_book_request(message, number: int) -> None:
    session = await asyncio.to_thread(load_session, message.reply_to_message_id)
    if not session:
        await reply(message, "⌯ انتهت الجلسة")
        return
    books = session.get("books", [])
    if number < 1 or number > len(books):
        await reply(message, "⌯ رقم غير صحيح")
        return
    book = books[number - 1]
    ids = load_data().get("channel_ids", {})
    try:
        await flood_retry(
            app.copy_message,
            chat_id=GROUP_ID,
            from_chat_id=ids.get(book["chat"]) or book["chat"],
            message_id=book["msg_id"],
            caption="",
            reply_to_message_id=message.id,
        )
        logger.info(f"Sent book #{number}: {book['name']}")
    except Exception as e:
        logger.error(f"Copy failed [{book['chat']}/{book['msg_id']}]: {e}")
        try:
            await reply(message, "⌯ تعذر جلب الكتاب")
        except Exception:
            pass


async def on_group_message(client, message) -> None:
    # لا نتعامل إلا مع رسائل بوت الاستلام نفسه
    if not message.from_user or message.from_user.id != RECEIVER_BOT_ID:
        return
    text = (message.text or "").strip()
    if text.startswith(REQUEST_PREFIX):
        spawn(handle_search_request(message, text))
    elif message.reply_to_message_id and re.fullmatch(r"\d{1,4}", text):
        spawn(handle_book_request(message, int(text)))


# ==============================================================
#  التشغيل
# ==============================================================
async def main() -> None:
    global app, _search_lock
    init_mongodb()
    _search_lock = asyncio.Lock()
    app = Client(
        "book_session",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=SESSION_STR,
        workers=max(4, min(32, (os.cpu_count() or 1) * 4)),
        parse_mode=ParseMode.DISABLED,  # أسماء الكتب تظهر كما هي بدون تنسيق
        sleep_threshold=0,
    )
    app.add_handler(MessageHandler(
        on_group_message,
        filters.chat(GROUP_ID) & filters.text & filters.bot,
    ))
    await app.start()
    logger.info("Pyrogram connected - loading dialogs cache...")

    try:
        async def _load():
            async for _ in app.get_dialogs():
                pass
        await asyncio.wait_for(_load(), timeout=30)
    except Exception as e:
        logger.warning(f"Dialogs load: {e}")

    try:
        chat = await asyncio.wait_for(app.get_chat(GROUP_ID), timeout=15)
        logger.info(f"Group loaded: {chat.title} ({GROUP_ID})")
    except Exception as e:
        logger.warning(f"Could not load group: {e}")

    data = sync_channels_from_env()
    for ch in data.get("channels", []):
        await resolve_channel(ch)
        await asyncio.sleep(0.3)

    logger.info("Session bot ready")
    await idle()
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
