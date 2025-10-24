import os
import time
import random
import logging
import re
import asyncio
import aiohttp
import sqlite3
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv
import vk_api
from vk_api.botlongpoll import VkBotLongPoll, VkBotEventType
from vk_api.utils import get_random_id
from logging.handlers import RotatingFileHandler
from difflib import get_close_matches

# ======================
# CONFIG & ENVIRONMENT
# ======================
class Config:
    MAX_VK_MSG_LEN = 4000
    RATE_LIMIT_CALLS_PER_SECOND = 3.0
    LONGPOLL_WAIT = 25
    GPT_TIMEOUT = 18
    GPT_MAX_TOKENS = 1200
    GPT_RETRIES = 3
    CONTEXT_MAX_MESSAGES = 8
    CONTEXT_TTL_SEC = 1800
    CONTEXT_GC_INTERVAL = 300
    AUTO_POST_INTERVAL = 6 * 3600
    YANDEX_RPM_LIMIT = 60
    DB_PATH = "data/bot_data.db"
    LOG_PATH = "logs/bot.log"
    POST_THEMES = [
        "новинки конструкторов JAKI и Pantasy",
        "космические конструкторы NoBlox",
        "хобби для взрослых — сборка моделей",
        "подарки и коллекции фанатов LEGO и JAKI"
    ]

def validate_env():
    load_dotenv()
    keys = ["VK_GROUP_TOKEN", "VK_GROUP_ID", "YANDEX_FOLDER_ID", "YANDEX_API_KEY"]
    env = {k: os.getenv(k) for k in keys}
    missing = [k for k, v in env.items() if not v]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")
    env["VK_GROUP_ID"] = int(env["VK_GROUP_ID"])
    return env

env = validate_env()
VK_TOKEN = env["VK_GROUP_TOKEN"]
VK_GROUP_ID = env["VK_GROUP_ID"]
FOLDER_ID = env["YANDEX_FOLDER_ID"]
YANDEX_API_KEY = env["YANDEX_API_KEY"]

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)
os.makedirs("data/images", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(Config.LOG_PATH, maxBytes=10_000_000, backupCount=5)
    ]
)
log = logging.getLogger(__name__)

# ======================
# DATABASE (Контексты и Автопосты)
# ======================
class BotDb:
    _local = threading.local()
    def __init__(self, path=Config.DB_PATH):
        self.path = path
        self._init()
    def _conn(self):
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn
    def _init(self):
        c = self._conn().cursor()
        c.execute("CREATE TABLE IF NOT EXISTS user_context(user_id INTEGER, ctx TEXT, ts REAL)")
        c.execute("CREATE TABLE IF NOT EXISTS post_schedule(ts INTEGER, theme TEXT, status TEXT, image TEXT)")
        self._conn().commit()
    def save_context(self, user_id, ctx):
        data = json.dumps(ctx, ensure_ascii=False)
        self.execute("DELETE FROM user_context WHERE user_id=?", (user_id,))
        self.execute("INSERT INTO user_context(user_id, ctx, ts) VALUES (?, ?, ?)", (user_id, data, time.time()))
    def load_context(self, user_id):
        resp = self.fetch_all("SELECT ctx FROM user_context WHERE user_id=? ORDER BY ts DESC LIMIT 1", (user_id,))
        return json.loads(resp[0]["ctx"]) if resp else []
    def gc_context(self):
        lim = time.time() - Config.CONTEXT_TTL_SEC
        self.execute("DELETE FROM user_context WHERE ts<?", (lim,))
    def schedule_post(self, ts, theme, status="new", image=None):
        self.execute("INSERT INTO post_schedule(ts, theme, status, image) VALUES (?, ?, ?, ?)", (ts, theme, status, image))
    def get_next_post(self):
        posts = self.fetch_all("SELECT rowid,* FROM post_schedule WHERE status='new' ORDER BY ts ASC LIMIT 1")
        return posts[0] if posts else None
    def set_post_status(self, rowid, status, image=None):
        self.execute("UPDATE post_schedule SET status=?, image=? WHERE rowid=?", (status, image, rowid))
    def execute(self, q, p=()):
        try:
            cur = self._conn().cursor()
            cur.execute(q, p)
            self._conn().commit()
            return cur
        except Exception as e:
            log.error("DB error: %s", e)
            self._conn().rollback()
            return None
    def fetch_all(self, q, p=()):
        cur = self.execute(q, p)
        return cur.fetchall() if cur else []

# ======================
# Рейт-лимитеры (VK + Yandex)
# ======================
class AsyncRateLimiter:
    def __init__(self, rps):
        self.min_interval = 1.0 / rps
        self.last = 0
        self.lock = asyncio.Lock()
    async def wait(self):
        async with self.lock:
            diff = time.time() - self.last
            if diff < self.min_interval:
                await asyncio.sleep(self.min_interval - diff)
            self.last = time.time()

# ======================
# VK API
# ======================
class VkApiManager:
    def __init__(self, token, group_id):
        self.group_id = group_id
        self.session = vk_api.VkApi(token=token)
        self.vk = self.session.get_api()
        self.longpoll = VkBotLongPoll(self.session, group_id, wait=Config.LONGPOLL_WAIT)
        self.upload = vk_api.VkUpload(self.session)
    async def send(self, peer_id, text):
        await asyncio.sleep(random.uniform(0.3, 1.1))  # “живость”
        self.vk.messages.send(peer_id=peer_id, random_id=get_random_id(), message=text[:Config.MAX_VK_MSG_LEN])
    def post_wall(self, text, photo_attach=None):
        params = {"owner_id": -self.group_id, "from_group": 1, "message": text}
        if photo_attach:
            params["attachments"] = photo_attach
        self.vk.wall.post(**params)
    def upload_wall_photo(self, image_path):
        resp = self.upload.photo_wall(photos=image_path, group_id=self.group_id)
        return "photo{}_{}".format(resp[0]["owner_id"], resp[0]["id"])

# ======================
# GPT+ART сервисы
# ======================
class YandexAiClient:
    def __init__(self, folder, key):
        self.folder = folder
        self.key = key
        self.session = None
    async def ensure(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
    async def chat(self, prompt, context=[]):
        await self.ensure()
        body = {
            "modelUri": f"gpt://{self.folder}/yandexgpt/latest",
            "completionOptions": {"stream": False, "temperature": 0.56, "maxTokens": Config.GPT_MAX_TOKENS},
            "messages": [{"role": "system", "text": "Ты — дружелюбный ассистент по конструкторам, пиши лаконично и по делу."}] + context + [{"role": "user", "text": prompt}]
        }
        for _ in range(Config.GPT_RETRIES):
            try:
                async with self.session.post(
                    "https://llm.api.cloud.yandex.net/foundationModels/v1/completion",
                    headers={"Authorization": f"Api-Key {self.key}"},
                    json=body,
                    timeout=aiohttp.ClientTimeout(total=Config.GPT_TIMEOUT)
                ) as r:
                    data = await r.json()
                    return data["result"]["alternatives"][0]["message"]["text"]
            except Exception as e:
                log.warning("GPT fail: %s", e)
                await asyncio.sleep(2)
        return "Не могу ответить, попробуйте ещё раз позже."
    async def generate_image(self, prompt):
        await self.ensure()
        body = {
            "modelUri": f"art://{self.folder}/yandex-art/latest",
            "generationOptions": {"seed": random.randint(100, 999999)},
            "messages": [{"role": "user", "text": prompt}]
        }
        try:
            async with self.session.post(
                "https://llm.api.cloud.yandex.net/foundationModels/v1/imageGenerationAsync",
                headers={"Authorization": f"Api-Key {self.key}"},
                json=body
            ) as resp:
                data = await resp.json()
                op = data.get("id")
                for _ in range(24):
                    await asyncio.sleep(5)
                    async with self.session.get(
                        f"https://llm.api.cloud.yandex.net/operations/{op}",
                        headers={"Authorization": f"Api-Key {self.key}"}
                    ) as st:
                        res = await st.json()
                        if res.get("done") and "image" in res["response"]:
                            img_url = res["response"]["image"]
                            async with self.session.get(img_url) as imgr:
                                return await imgr.read()
        except Exception as e:
            log.error("Image gen error: %s", e)
        return None

# ======================
# Контент-генерация и Поиск
# ======================
class ProductIndex:
    def __init__(self, product_dict):
        self.alias_map = {}
        for k, v in product_dict.items():
            names = [k] + v.get("aliases", [])
            for name in names:
                self.alias_map[name.lower()] = v["desc"]
    def search(self, text):
        text = text.lower()
        match = get_close_matches(text, self.alias_map.keys(), n=1, cutoff=0.4)
        if match:
            return self.alias_map[match[0]]
        for k in self.alias_map:
            if k in text:
                return self.alias_map[k]
        return None

# ======================
# Автопостинг менеджер
# ======================
class AutoPostingManager:
    def __init__(self, db: BotDb, vk: VkApiManager, ai: YandexAiClient):
        self.db = db
        self.vk = vk
        self.ai = ai
    async def scheduler(self):
        while True:
            now = int(time.time())
            next_post = self.db.get_next_post()
            if not next_post:
                # Запланировать новый пост
                ts = now + Config.AUTO_POST_INTERVAL
                theme = random.choice(Config.POST_THEMES)
                self.db.schedule_post(ts, theme)
                await asyncio.sleep(60)
                continue
            if next_post["ts"] <= now:
                await self.publish_post(next_post)
            else:
                await asyncio.sleep(min(300, next_post["ts"]-now))
    async def publish_post(self, post_row):
        try:
            theme = post_row["theme"]
            text = await self.ai.chat(f"Напиши продающий пост по теме: {theme}", [])
            image_raw = await self.ai.generate_image(theme)
            photo_attach = None
            if image_raw:
                fname = f"data/images/post_{int(time.time())}.jpg"
                with open(fname, "wb") as f:
                    f.write(image_raw)
                photo_attach = self.vk.upload_wall_photo(fname)
            self.vk.post_wall(text, photo_attach)
            self.db.set_post_status(post_row["rowid"], "posted", photo_attach)
            log.info("Опубликован пост на тему: %s", theme)
        except Exception as e:
            log.error("Ошибка автопостинга: %s", e)
            self.db.set_post_status(post_row["rowid"], "failed", None)

# ======================
# Главный класс VKBot
# ======================
class VKBot:
    def __init__(self):
        self.db = BotDb()
        self.vk = VkApiManager(VK_TOKEN, VK_GROUP_ID)
        self.ai = YandexAiClient(FOLDER_ID, YANDEX_API_KEY)
        self.vk_rate = AsyncRateLimiter(Config.RATE_LIMIT_CALLS_PER_SECOND)
        self.index = ProductIndex({
            "astronaut": {"desc": "JAKI Astronaut — топовый набор космонавта!", "aliases": ["астробой", "астронавт"]},
            "pantasy": {"desc": "Pantasy — коллекционные премиум-наборы.", "aliases": []},
            "space": {"desc": "NoBlox Space — спутники, ракеты, космос.", "aliases": ["космос", "ракета", "шаттл"]},
            "rocket": {"desc": "JAKI Rocket — культовая ракета!", "aliases": ["ракета", "space-rocket"]},
        })
        self.autopost = AutoPostingManager(self.db, self.vk, self.ai)
    async def handle_message(self, user_id, peer_id, text):
        try:
            prod = self.index.search(text)
            if prod:
                await self.vk_rate.wait()
                await self.vk.send(peer_id, prod + "\n👉 Каталог на стене VK.")
                return
            if any(x in text.lower() for x in ["цена", "сколько", "прайс"]):
                await self.vk_rate.wait()
                await self.vk.send(peer_id, "Цены и наличие — в разделе 'Товары VK'.")
                return
            ctx = self.db.load_context(user_id)
            reply = await self.ai.chat(text, ctx)
            ctx = (ctx + [{"role": "user", "text": text}, {"role": "assistant", "text": reply}])[-Config.CONTEXT_MAX_MESSAGES:]
            self.db.save_context(user_id, ctx)
            await self.vk_rate.wait()
            await self.vk.send(peer_id, reply)
        except Exception as e:
            log.error("Ошибка обработки сообщения: %s", e)
            await self.vk.send(peer_id, "Извините, сейчас не могу ответить.")
    async def run(self):
        asyncio.create_task(self.autopost.scheduler())
        while True:
            try:
                for event in self.vk.longpoll.check():
                    if event.type == VkBotEventType.MESSAGE_NEW:
                        msg = event.object.message
                        user_id, peer_id, text = msg["from_id"], msg["peer_id"], msg.get("text", "")
                        if text:
                            asyncio.create_task(self.handle_message(user_id, peer_id, text))
            except Exception as e:
                log.error("Longpoll error: %s", e)
                await asyncio.sleep(5)

async def main():
    bot = VKBot()
    await bot.run()

if __name__ == '__main__':
    asyncio.run(main())
