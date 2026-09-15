import base64
import datetime
import hashlib
import hmac
import uuid
from typing import Any, Dict, List, Optional, Tuple

import urllib3
import aiohttp

from config import PANEL_API_TOKEN, PANEL_URL, TRUE_SUB_LINK, MIRROR_SUB_LINK, SHORT_UUID_SECRET
from config_bd.utils import AsyncSQL
from logging_config import logger
from payments.tariff_gate import add_tariff_period, tariff_period_label
import random
import string

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def panel_username_for_site_email(email_norm: str, is_white: bool) -> str:
    e = (email_norm or "").strip().lower()
    digest = hashlib.sha256(e.encode("utf-8")).hexdigest()[:24]
    return f"em_{digest}_m" if is_white else f"em_{digest}"


def panel_username_for_site_user(db_user_id: int, is_white: bool) -> str:
    n = int(db_user_id)
    base = str(n)
    if len(base) < 3:
        base = f"n{n}"
    return f"{base}_white" if is_white else base


def gift_panel_username(gift_num: int, is_white: bool) -> str:
    """Username в панели для веб-подарка: gift_N или gift_N_white."""
    base = f"gift_{gift_num}"
    return f"{base}_white" if is_white else base


class X3:
    def __init__(self):
        """Инициализация класса с настройками подключения"""
        self.target_url = PANEL_URL
        self.api_token = PANEL_API_TOKEN
        
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_token}'
        }
        
        self.params = {
            "vyWdoTBH": "VmsLiQrN"
        }

        self._session: aiohttp.ClientSession = None
        self.working_host = self.target_url
        self.is_authenticated = True

    async def _get_session(self) -> aiohttp.ClientSession:
        """Возвращает активную сессию aiohttp, создавая её при необходимости."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(ssl=False)
            self._session = aiohttp.ClientSession(
                headers=self.headers,
                connector=connector
            )
        return self._session

    async def close(self):
        """Закрывает сессию aiohttp (вызывать при завершении работы)."""
        if self._session and not self._session.closed:
            await self._session.close()

    def generate_client_id(self, tg_id):
        """shortUuid: HMAC-SHA256(секрет, tg_id), 15 символов; white — тот же метод с tg_id*100."""
        if not SHORT_UUID_SECRET:
            raise ValueError(
                "SHORT_UUID_SECRET не задан в окружении (.env) — нужен для генерации shortUuid"
            )
        key = SHORT_UUID_SECRET.encode("utf-8")
        msg = str(int(tg_id)).encode("utf-8")
        digest = hmac.new(key, msg, hashlib.sha256).digest()
        token = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        return token[:15]

    def list_from_host(self, host):
        """Заглушка для совместимости со старым кодом"""
        return {'obj': [{'settings': '{"clients": []}'}]}

    async def test_connect(self):
        try:
            session = await self._get_session()
            async with session.get(
                    f"{self.target_url}/api/auth/status",
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                logger.info(f"Тест подключения: {response.status}")
                return response.status == 200
        except Exception as e:
            logger.error(f"Ошибка подключения: {e}")
            return False

    async def list(self, start):
        try:
            params = self.params
            params['size'] = 1000
            params['start'] = start
            session = await self._get_session()
            async with session.get(
                    f'{self.target_url}/api/users',
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 200:
                    logger.info(f'Получены юзеры с {start}')
                    return await resp.json()
                else:
                    logger.error(f"HTTP {resp.status}: {await resp.text()}")
                    return {'response': {'users': []}}
        except Exception as e:
            logger.error(f"Ошибка запроса: {e}")
            return {'response': {'users': []}}

    def _generate_password(self, length=12):
        """Генерирует случайный пароль"""
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for _ in range(length))

    def _site_password_from_email(self, email_norm: str, purpose: str) -> str:
        if not SHORT_UUID_SECRET:
            raise ValueError(
                "SHORT_UUID_SECRET не задан в окружении (.env) — нужен для паролей site-клиента"
            )
        key = str(SHORT_UUID_SECRET).encode("utf-8")
        msg = f"{purpose}|{email_norm}".encode("utf-8")
        digest = hmac.new(key, msg, hashlib.sha256).digest()
        raw = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        return (raw + "Aa1")[:16]

    def generate_site_short_uuid(self, email_norm: str, is_white: bool, db_user_id: int) -> str:
        if not SHORT_UUID_SECRET:
            raise ValueError(
                "SHORT_UUID_SECRET не задан в окружении (.env) — нужен для shortUuid site-клиента"
            )
        key = str(SHORT_UUID_SECRET).encode("utf-8")
        tag = b"|white|1" if is_white else b"|white|0"
        msg = (
            email_norm.encode("utf-8")
            + b"\x00uid\x00"
            + str(int(db_user_id)).encode("utf-8")
            + tag
        )
        digest = hmac.new(key, msg, hashlib.sha256).digest()
        token = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        return token[:15]

    async def add_client_site(self, day, email_norm: str, is_white: bool, db_user_id: int):
        try:
            email_key = (email_norm or "").strip().lower()
            panel_username = panel_username_for_site_user(db_user_id, is_white)
            client_id = self.generate_site_short_uuid(email_key, is_white, db_user_id)
            current_time = datetime.datetime.now(datetime.timezone.utc)
            expire_time = add_tariff_period(current_time, day)
            vless_uuid = str(uuid.uuid1())

            if is_white:
                squad = ['2be165be-24d1-471e-a1bb-eb57bb45f9b7']
                traffic_limit_strategy = "MONTH"
                traffic_limit_bytes = 80530636800
                hwid_device_limit = 1
            else:
                squad_1 = ['28b6a3bf-8e81-42dd-9ac8-dab1c9a60b0a']
                squad_2 = ['85f8520a-8dd7-40a6-9f27-8a7467096c6a']
                squad = random.choice([squad_1, squad_2])
                traffic_limit_strategy = "NO_RESET"
                traffic_limit_bytes = 0
                hwid_device_limit = 5

            data = {
                "username": panel_username,
                "status": "ACTIVE",
                "shortUuid": client_id,
                "trojanPassword": self._site_password_from_email(email_key, "trojan"),
                "vlessUuid": vless_uuid,
                "ssPassword": self._site_password_from_email(email_key, "ss"),
                "trafficLimitStrategy": traffic_limit_strategy,
                "trafficLimitBytes": traffic_limit_bytes,
                "expireAt": expire_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                "createdAt": current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                "hwidDeviceLimit": hwid_device_limit,
                "telegramId": int(db_user_id),
                "description": "Ускоритель соцсетей",
                "activeInternalSquads": squad
            }

            logger.info(f"Добавление site-клиента {panel_username}, срок до: {expire_time}")
            session = await self._get_session()
            async with session.post(
                    f"{self.target_url}/api/users",
                    json=data,
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                logger.info(f"Код ответа add_client_site: {response.status}")
                if response.status in [200, 201]:
                    sql = AsyncSQL()
                    try:
                        response_data = await response.json()
                    except (aiohttp.ClientConnectionError, aiohttp.ContentTypeError, ValueError) as e:
                        logger.warning(
                            f"Не удалось прочитать JSON при add_client_site {db_user_id}: {e}. Считаем успехом."
                        )
                        subscription_end_date = expire_time.replace(tzinfo=datetime.timezone.utc)
                        if not is_white:
                            await sql.update_subscription_end_date(db_user_id, subscription_end_date)
                            await sql.update_subscribtion(db_user_id, client_id)
                        return True
                    else:
                        if response_data.get("success", True):
                            subscription_end_date = expire_time.replace(tzinfo=datetime.timezone.utc)
                            if not is_white:
                                await sql.update_subscription_end_date(db_user_id, subscription_end_date)
                                await sql.update_subscribtion(db_user_id, client_id)
                            logger.info(f"✅ Site-клиент {panel_username} добавлен")
                            return True
                        logger.warning(f"❌ API add_client_site: {response_data}")
                        return False
                error_text = await response.text() if response.content else "No body"
                logger.error(f"❌ add_client_site HTTP {response.status} - {error_text}")
                return False
        except Exception as e:
            logger.error(f"❌ add_client_site {panel_username}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def delete_panel_user_by_username(self, username: str) -> bool:
        try:
            user_response = await self.get_user_by_username(username)
            if not user_response or 'response' not in user_response or not user_response['response']:
                return True
            user = self._panel_user_from_response(user_response)
            panel_user_id = self._panel_user_id(user)
            if not panel_user_id:
                return True
            session = await self._get_session()
            async with session.delete(
                    f"{self.target_url}/api/users/{panel_user_id}",
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                if response.status in (200, 204, 404):
                    logger.info(f"Панель: удалён пользователь {username} (id={panel_user_id})")
                    return True
                error_text = await response.text() if response.content else "No body"
                logger.warning(f"Удаление {username} из панели: HTTP {response.status} {error_text}")
                return False
        except Exception as e:
            logger.warning(f"delete_panel_user_by_username {username}: {e}")
            return False

    async def addClient(self, day, user_id_str, user_id):
        """Добавляет нового клиента"""
        try:
            client_id = self.generate_client_id(user_id)
            if 'white' in user_id_str:
                client_id = self.generate_client_id(user_id * 100)
            current_time = datetime.datetime.now(datetime.timezone.utc)
            expire_time = add_tariff_period(current_time, day)
            vless_uuid = str(uuid.uuid1())

            if 'white' in user_id_str:
                squad_1 = ['2be165be-24d1-471e-a1bb-eb57bb45f9b7']
                squad = squad_1
                trafficLimitStrategy = "MONTH"
                trafficLimitBytes = 80530636800
                hwidDeviceLimit = 1
            else:
                squad_1 = ['28b6a3bf-8e81-42dd-9ac8-dab1c9a60b0a']
                squad_2 = ['85f8520a-8dd7-40a6-9f27-8a7467096c6a']
                squad = random.choice([squad_1, squad_2])
                trafficLimitStrategy = "NO_RESET"
                trafficLimitBytes = 0
                hwidDeviceLimit = 5
            desc = 'Ускоритель соцсетей'
            data = {
                "username": user_id_str,
                "status": "ACTIVE",
                "shortUuid": client_id,
                "trojanPassword": self._generate_password(),
                "vlessUuid": vless_uuid,
                "ssPassword": self._generate_password(),
                "trafficLimitStrategy": trafficLimitStrategy,
                "trafficLimitBytes": trafficLimitBytes,
                "expireAt": expire_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                "createdAt": current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                "hwidDeviceLimit": hwidDeviceLimit,
                "telegramId": int(user_id),
                "description": desc,
                "activeInternalSquads": squad
            }

            logger.info(f"Добавление клиента {user_id_str}, срок до: {expire_time}")

            session = await self._get_session()
            async with session.post(
                    f"{self.target_url}/api/users",
                    json=data,
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                logger.info(f"Код ответа: {response.status}")

                if response.status in [200, 201]:
                    sql = AsyncSQL()
                    try:
                        response_data = await response.json()
                    except (aiohttp.ClientConnectionError, aiohttp.ContentTypeError, ValueError) as e:
                        # Сервер мог не вернуть JSON, но статус успешный
                        logger.warning(f"Не удалось прочитать JSON при добавлении {user_id}: {e}. Считаем успехом.")
                        subscription_end_date = expire_time.replace(tzinfo=datetime.timezone.utc)
                        if 'white' not in user_id_str:
                            await sql.update_subscription_end_date(user_id, subscription_end_date)
                            await sql.update_subscribtion(user_id, client_id)
                        logger.info(f"✅ Клиент {user_id} успешно добавлен (без JSON)")
                        return True
                    else:
                        if response_data.get("success", True):
                            subscription_end_date = expire_time.replace(tzinfo=datetime.timezone.utc)
                            if 'white' not in user_id_str:
                                await sql.update_subscription_end_date(user_id, subscription_end_date)
                                await sql.update_subscribtion(user_id, client_id)
                            logger.info(f"✅ Клиент {user_id} успешно добавлен")
                            return True
                        else:
                            logger.warning(f"❌ API вернул ошибку: {response_data}")
                            return False
                else:
                    error_text = await response.text() if response.content else "No body"
                    logger.error(f"❌ Ошибка добавления клиента: HTTP {response.status} - {error_text}")
                    return False

        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении клиента {user_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    @staticmethod
    def _panel_iso_z(dt: datetime.datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    async def add_client_migrate(
        self,
        user_id: int,
        expire_at: datetime.datetime,
        short_uuid: str | None = None,
    ) -> bool:
        """Создаёт пользователя в панели по данным миграции (/add_new_users)."""
        user_id_str = str(int(user_id))
        try:
            client_id = (short_uuid or "").strip() or self.generate_client_id(user_id)
            current_time = datetime.datetime.now(datetime.timezone.utc)
            vless_uuid = str(uuid.uuid1())
            squad_1 = ['28b6a3bf-8e81-42dd-9ac8-dab1c9a60b0a']
            squad_2 = ['85f8520a-8dd7-40a6-9f27-8a7467096c6a']
            squad = random.choice([squad_1, squad_2])
            data = {
                "username": user_id_str,
                "status": "ACTIVE",
                "shortUuid": client_id,
                "trojanPassword": self._generate_password(),
                "vlessUuid": vless_uuid,
                "ssPassword": self._generate_password(),
                "trafficLimitStrategy": "NO_RESET",
                "trafficLimitBytes": 0,
                "expireAt": self._panel_iso_z(expire_at),
                "createdAt": self._panel_iso_z(current_time),
                "hwidDeviceLimit": 5,
                "telegramId": int(user_id),
                "description": "SocialMedia",
                "activeInternalSquads": squad,
            }
            logger.info(
                f"add_client_migrate {user_id_str}, expireAt={data['expireAt']}, shortUuid={client_id}"
            )
            session = await self._get_session()
            async with session.post(
                    f"{self.target_url}/api/users",
                    json=data,
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                logger.info(f"add_client_migrate {user_id_str}: HTTP {response.status}")
                if response.status not in (200, 201):
                    error_text = await response.text() if response.content else "No body"
                    logger.error(
                        f"add_client_migrate {user_id_str}: HTTP {response.status} - {error_text}"
                    )
                    return False
                try:
                    response_data = await response.json()
                except (aiohttp.ClientConnectionError, aiohttp.ContentTypeError, ValueError):
                    response_data = {"success": True}
                if not response_data.get("success", True):
                    logger.warning(f"add_client_migrate API: {response_data}")
                    return False
                sql = AsyncSQL()
                await sql.update_in_panel(user_id)
                if not (short_uuid or "").strip():
                    await sql.update_subscribtion(user_id, client_id)
                logger.info(f"add_client_migrate: OK {user_id_str}")
                return True
        except Exception as e:
            logger.error(f"add_client_migrate {user_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def updateClient(self, day, user_id_str, user_id):
        """Обновляет клиента — добавляет период тарифа к подписке"""
        try:
            # Получаем данные пользователя
            user_response = await self.get_user_by_username(user_id_str)

            if not user_response or 'response' not in user_response:
                logger.error(f"❌ Пользователь {user_id_str} не найден")
                return False

            user = self._panel_user_from_response(user_response)
            if not user:
                logger.error(f"❌ Пользователь {user_id_str} не найден")
                return False

            panel_user_id = self._panel_user_id(user)
            if panel_user_id is None or 'expireAt' not in user:
                logger.error(f"❌ У пользователя {user_id_str} отсутствуют обязательные поля (id/expireAt)")
                return False
            
            # Парсим текущую дату истечения
            expire_at_str = user['expireAt']
            current_expire_at = datetime.datetime.fromisoformat(expire_at_str.replace('Z', '+00:00'))
            now = datetime.datetime.now(datetime.timezone.utc)

            period = tariff_period_label(day)
            # Определяем новую дату истечения
            if current_expire_at < now:
                # Подписка истекла - начинаем с текущего момента
                new_expire_at = add_tariff_period(now, day)
                status = 'ACTIVE'  # Активируем подписку
                logger.info(f"Подписка пользователя {user_id_str} истекла. Активируем и добавляем {period}")
            else:
                # Подписка активна - добавляем к существующей дате
                new_expire_at = add_tariff_period(current_expire_at, day)
                status = user.get('status', 'ACTIVE')
                logger.info(f"Подписка пользователя {user_id_str} активна. Добавляем {period}")

            # Обрабатываем activeInternalSquads
            raw_squads = user.get('activeInternalSquads', [])
            squads = []
            for s in raw_squads:
                if isinstance(s, dict) and 'uuid' in s:
                    squads.append(s['uuid'])
                elif isinstance(s, str):
                    squads.append(s)

            # Формируем данные для обновления
            data = {
                "id": panel_user_id,
                "status": status,
                "expireAt": new_expire_at.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                "trafficLimitBytes": user.get('trafficLimitBytes', 0),
                "trafficLimitStrategy": user.get('trafficLimitStrategy', "NO_RESET"),
                "activeInternalSquads": squads
            }

            logger.info(f"Обновление пользователя {user_id_str}:")
            logger.info(f"  Старая дата: {current_expire_at.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"  Новая дата: {new_expire_at.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"  Добавлено: {period}")

            session = await self._get_session()
            async with session.patch(
                    f"{self.target_url}/api/users",
                    json=data,
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                logger.info(f"Код ответа updateClient: {response.status}")
                if response.status == 200:
                    sql = AsyncSQL()
                    try:
                        response_data = await response.json()
                    except (aiohttp.ClientConnectionError, aiohttp.ContentTypeError, ValueError) as e:
                        logger.warning(f"Не удалось прочитать JSON при обновлении {user_id}: {e}. Считаем успехом.")
                        if 'white' not in user_id_str:
                            await sql.update_subscription_end_date(user_id, new_expire_at)
                        logger.info(f"✅ Клиент {user_id} успешно обновлён (без JSON), добавлено {day} дней")
                        return True
                    else:
                        if response_data.get("success", True):
                            if 'white' not in user_id_str:
                                await sql.update_subscription_end_date(user_id, new_expire_at)
                            logger.info(f"✅ Клиент {user_id} успешно обновлён, добавлено {day} дней")
                            return True
                        else:
                            logger.error(f"❌ API вернул success=false: {response_data}")
                            return False
                else:
                    error_text = await response.text() if response.content else "No body"
                    logger.error(f"❌ Ошибка обновления: HTTP {response.status}, {error_text}")
                    return False

        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении клиента {user_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def get_user_by_username(self, username):
        try:
            session = await self._get_session()
            async with session.get(
                    f"{self.target_url}/api/users/by-username/{username}",
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    try:
                        return await resp.json()
                    except:
                        logger.error(f"Не удалось прочитать JSON для пользователя {username}")
                        return None
                else:
                    logger.error(f"Ошибка получения пользователя {username}: {await resp.text()}")
                    return None
        except Exception as e:
            logger.error(f"Ошибка получения пользователя {username}: {e}")
            return None

    async def get_user_by_telegram_id(self, telegram_id):
        try:
            session = await self._get_session()
            async with session.get(
                    f"{self.target_url}/api/users/by-telegram-id/{telegram_id}",
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    try:
                        return await resp.json()
                    except:
                        return None
                else:
                    return None
        except Exception as e:
            logger.error(f"Ошибка получения пользователя по telegram_id {telegram_id}: {e}")
            return None

    async def sublink(self, user_id: str):
        try:
            users = await self.get_user_by_username(user_id)
            if users and 'response' in users and users['response']:
                user = users['response']
                true_sublink = user.get('subscriptionUrl', '')
                mirror_sublink = true_sublink.replace(TRUE_SUB_LINK, MIRROR_SUB_LINK)
                return mirror_sublink
        except Exception as e:
            logger.error(f"Ошибка при получении ссылки для {user_id}: {e}")
        return ""

    SUBSCRIPTION_SLOTS: Tuple[Tuple[str, str, str], ...] = (
        ("main", "", "💫 Подписка PRO — соцсети"),
        ("white", "_white", "📱 Мобильный тариф"),
    )

    @staticmethod
    def _panel_user_from_response(users: Optional[dict]) -> Optional[dict]:
        if not users or 'response' not in users or not users['response']:
            return None
        raw = users['response']
        return raw[0] if isinstance(raw, list) else raw

    @staticmethod
    def _panel_user_id(user: Optional[dict]) -> Optional[int]:
        """Числовой id пользователя в панели (Remnawave больше не отдаёт uuid)."""
        if not user:
            return None
        pid = user.get('id')
        if pid is None:
            return None
        try:
            return int(pid)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _panel_user_is_active(user: dict) -> bool:
        expiry_time_str = user.get('expireAt')
        if not expiry_time_str:
            return False
        expiry_dt = datetime.datetime.fromisoformat(expiry_time_str.replace('Z', '+00:00'))
        now = datetime.datetime.now(datetime.timezone.utc)
        expiry_time = int(expiry_dt.timestamp() * 1000)
        current_time = int(now.timestamp() * 1000)
        return user.get('status') == 'ACTIVE' and expiry_time > current_time

    async def active_subscription_slots(
        self, telegram_id: int,
    ) -> List[Tuple[str, str, str, str]]:
        """Активные подписки: (ключ слота, подпись, id в панели, username)."""
        out: List[Tuple[str, str, str, str]] = []
        for slot_key, suffix, label in self.SUBSCRIPTION_SLOTS:
            username = f"{telegram_id}{suffix}"
            users = await self.get_user_by_username(username)
            user = self._panel_user_from_response(users)
            if not user or not self._panel_user_is_active(user):
                continue
            panel_user_id = self._panel_user_id(user)
            if panel_user_id is None:
                continue
            out.append((slot_key, label, str(panel_user_id), username))
        return out

    async def get_user_hwid_devices(self, panel_user_id: str) -> Tuple[List[Dict[str, Any]], int]:
        """Список HWID-устройств пользователя и их количество."""
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.target_url}/api/hwid/devices/{panel_user_id}",
                params=self.params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    logger.error(
                        f"get_user_hwid_devices {panel_user_id}: HTTP {resp.status} — {await resp.text()}"
                    )
                    return [], 0
                data = await resp.json()
        except Exception as e:
            logger.error(f"get_user_hwid_devices {panel_user_id}: {e}")
            return [], 0

        response = data.get('response') if isinstance(data, dict) else None
        if isinstance(response, list):
            devices = response
            total = len(devices)
        elif isinstance(response, dict):
            devices = response.get('devices') or []
            total = response.get('total', len(devices))
        else:
            devices = []
            total = 0
        return devices, int(total)

    async def delete_user_hwid_device(self, panel_user_id: str, hwid: str) -> bool:
        """Удаляет одно HWID-устройство пользователя."""
        try:
            session = await self._get_session()
            async with session.post(
                f"{self.target_url}/api/hwid/devices/delete",
                json={"userId": int(panel_user_id), "hwid": hwid},
                params=self.params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    logger.error(
                        f"delete_user_hwid_device {panel_user_id}: HTTP {resp.status} — {await resp.text()}"
                    )
                    return False
                data = await resp.json()
                if isinstance(data, dict) and data.get('success') is False:
                    logger.error(f"delete_user_hwid_device API: {data}")
                    return False
                return True
        except Exception as e:
            logger.error(f"delete_user_hwid_device {panel_user_id}: {e}")
            return False

    async def activ(self, user_id: str):
        result = {'activ': '🔎 - Не подключён', 'time': '-'}
        try:
            users = await self.get_user_by_username(user_id)
            if not users or 'response' not in users or not users['response']:
                logger.info(f"Пользователь {user_id} не найден в системе")
                return result

            raw = users['response']
            user = raw[0] if isinstance(raw, list) else raw
            current_time = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

            expiry_time_str = user.get('expireAt')
            if not expiry_time_str:
                return result

            expiry_dt = datetime.datetime.fromisoformat(expiry_time_str.replace('Z', '+00:00'))
            expiry_time = int(expiry_dt.timestamp() * 1000)

            expiry_dt_msk = expiry_dt + datetime.timedelta(hours=3)
            readable_time = expiry_dt_msk.strftime('%d-%m-%Y %H:%M') + ' МСК'
            result['time'] = readable_time

            if user.get('status') == 'ACTIVE' and expiry_time > current_time:
                result['activ'] = '✅ - Активен'
            else:
                result['activ'] = '❌ - Не Активен'

            return result

        except Exception as e:
            logger.error(f"Ошибка в методе activ для {user_id}: {e}")
            result['activ'] = '❌ - Внутренняя ошибка'
            return result

    async def activ_list(self):
        lst_users = []
        try:
            users_all = []
            for i in range(200):
                data = await self.list(1000 * i + 1)
                if data['response']['users']:
                    users_all.extend(data['response']['users'])
                else:
                    break
            logger.info(f'Всего юзеров в панели - {len(users_all)}')
            for user in users_all:
                if user.get('userTraffic', {}).get('firstConnectedAt'):
                    telegram_id = user.get('telegramId')
                    if telegram_id is not None:
                        lst_users.append(int(telegram_id))
            logger.info(f'Всего юзеров подключенных - {len(lst_users)}')
        except Exception as e:
            logger.error(f"Ошибка при получении списка активности: {e}")
        return lst_users

    async def get_all_users(self):
        """
        Возвращает список всех пользователей из панели (объекты пользователей),
        у которых description == 'New user - without pay'.
        """
        users_all = []
        try:
            for i in range(200):  # максимум 50 страниц
                data = await self.list(1000 * i + 1)
                if data['response']['users']:
                    users_all.extend(data['response']['users'])
                else:
                    break
            logger.info(f'Всего юзеров в панели - {len(users_all)}')
        except Exception as e:
            logger.error(f"Ошибка при получении всех пользователей: {e}")
        return users_all

    async def list_nodes(self) -> List[dict]:
        """GET /api/nodes — список нод панели."""
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.target_url}/api/nodes",
                params=self.params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                if response.status != 200:
                    err = (await response.text())[:300]
                    logger.warning(f"list_nodes: HTTP {response.status}: {err}")
                    return []
                data = await response.json()
                resp = data.get("response") or data
                if isinstance(resp, list):
                    return resp
                if isinstance(resp, dict):
                    return resp.get("nodes") or []
                return []
        except Exception as e:
            logger.error(f"list_nodes: {e}")
            return []

    async def get_node_uuid_by_name(self, node_name: str) -> Optional[str]:
        """UUID ноды по имени (кэш на время жизни экземпляра X3)."""
        cache: Dict[str, str] = getattr(self, "_node_uuid_by_name", {})
        if node_name in cache:
            return cache[node_name]

        for node in await self.list_nodes():
            name = node.get("name") or node.get("nodeName") or ""
            if name != node_name:
                continue
            node_uuid = node.get("uuid") or node.get("nodeUuid")
            if node_uuid:
                cache[node_name] = str(node_uuid)
                self._node_uuid_by_name = cache
                return cache[node_name]

        logger.warning(f"get_node_uuid_by_name: нода «{node_name}» не найдена")
        return None

    @staticmethod
    def _bandwidth_users_records_from_response(data: dict) -> Optional[List[dict]]:
        resp = data.get("response") or data
        if isinstance(resp, list):
            return resp
        if isinstance(resp, dict):
            top_users = resp.get("topUsers")
            if isinstance(top_users, list):
                return top_users
            series = resp.get("series")
            if isinstance(series, list):
                return series
        return None

    async def get_node_users_bandwidth_legacy(
        self,
        node_uuid: str,
        start: str,
        end: str,
        *,
        top_users_limit: int = 5000,
    ) -> Optional[List[dict]]:
        """
        Bulk-трафик пользователей на ноде за период.
        GET /api/bandwidth-stats/nodes/{nodeUuid}/users → response.topUsers.
        """
        params = {
            **self.params,
            "start": start,
            "end": end,
            "topUsersLimit": str(top_users_limit),
        }
        url = f"{self.target_url}/api/bandwidth-stats/nodes/{node_uuid}/users"
        try:
            session = await self._get_session()
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as response:
                if response.status != 200:
                    err = (await response.text())[:300]
                    logger.error(
                        f"node users bandwidth {node_uuid}: HTTP {response.status}: {err}"
                    )
                    return None
                try:
                    data = await response.json()
                except (aiohttp.ContentTypeError, ValueError):
                    logger.error(f"node users bandwidth {node_uuid}: не удалось прочитать JSON")
                    return None
                records = self._bandwidth_users_records_from_response(data)
                if not records:
                    logger.error(
                        f"node users bandwidth {node_uuid}: ответ без topUsers/series"
                    )
                    return None
                logger.debug(
                    f"node users bandwidth {node_uuid}: получено {len(records)} записей"
                )
                return records
        except Exception as e:
            logger.error(f"node users bandwidth {node_uuid}: {e}")
            return None

    @staticmethod
    def aggregate_bandwidth_by_username(records: List[dict]) -> Dict[str, float]:
        """Суммирует total (байты) по username из legacy-ответа панели."""
        totals: Dict[str, float] = {}
        for item in records:
            if not isinstance(item, dict):
                continue
            username = str(item.get("username") or "")
            if not username:
                continue
            totals[username] = totals.get(username, 0.0) + float(item.get("total") or 0)
        return totals

    @staticmethod
    def aggregate_bandwidth_by_user_uuid(records: List[dict]) -> Dict[str, float]:
        """Суммирует total (байты) по userId из legacy-ответа панели."""
        totals: Dict[str, float] = {}
        for item in records:
            if not isinstance(item, dict):
                continue
            user_id = str(item.get("userId") or item.get("userUuid") or "")
            if not user_id:
                continue
            totals[user_id] = totals.get(user_id, 0.0) + float(item.get("total") or 0)
        return totals

    async def get_user_bandwidth_stats(self, panel_user_id: str, start: str, end: str) -> Optional[dict]:
        """
        GET /api/bandwidth-stats/users/{id}?start=&end=
        Возвращает тело ответа или None.
        """
        try:
            session = await self._get_session()
            params = {**self.params, "start": start, "end": end}
            async with session.get(
                f"{self.target_url}/api/bandwidth-stats/users/{panel_user_id}",
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                if response.status != 200:
                    err = (await response.text())[:300]
                    logger.warning(
                        f"bandwidth-stats/users {panel_user_id}: HTTP {response.status}: {err}"
                    )
                    return None
                try:
                    return await response.json()
                except (aiohttp.ContentTypeError, ValueError):
                    return None
        except Exception as e:
            logger.error(f"bandwidth-stats/users {panel_user_id}: {e}")
            return None

    async def get_user_node_bandwidth_bytes(
        self, panel_user_id: str, node_name: str,
    ) -> float:
        """Суммарный трафик пользователя на указанной ноде (байты)."""
        today = datetime.datetime.now(datetime.timezone.utc).date()
        start = "2024-01-01"
        end = today.isoformat()
        data = await self.get_user_bandwidth_stats(panel_user_id, start, end)
        if not data:
            return 0.0

        resp = data.get("response") or data
        if isinstance(resp, list):
            total = 0.0
            for item in resp:
                if not isinstance(item, dict):
                    continue
                name = item.get("nodeName") or item.get("name") or ""
                if name == node_name:
                    total += float(item.get("total") or 0)
            return total

        # topNodes и series содержат одни и те же total по ноде — не суммировать оба.
        for key in ("topNodes", "series"):
            for item in resp.get(key) or []:
                name = item.get("name") or item.get("nodeName") or ""
                if name == node_name:
                    return float(item.get("total") or 0)

        return 0.0

    async def update_user_squads(self, panel_user_id: int, squads: list):
        """
        Обновляет поле activeInternalSquads у пользователя по id в панели.
        :param panel_user_id: числовой id пользователя в панели
        :param squads: список squad UUID (например, ['2fcfd928-6f45-4a8c-a36b-742fca8efea0'])
        :return: True при успехе, False при ошибке
        """
        try:
            data = {
                "id": int(panel_user_id),
                "activeInternalSquads": squads
            }
            session = await self._get_session()
            async with session.patch(
                    f"{self.target_url}/api/users",
                    json=data,
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    try:
                        response_data = await response.json()
                    except (aiohttp.ClientConnectionError, aiohttp.ContentTypeError, ValueError) as e:
                        logger.warning(
                            f"Не удалось прочитать JSON при обновлении squads для id {panel_user_id}: {e}. Считаем успехом.")
                        return True
                    else:
                        if response_data.get("success", True):
                            logger.info(f"✅ Squad обновлён для id {panel_user_id}")
                            return True
                        else:
                            logger.error(f"❌ API вернул ошибку: {response_data}")
                            return False
                else:
                    error_text = await response.text() if response.content else "No body"
                    logger.error(f"❌ Ошибка HTTP {response.status}: {error_text}")
                    return False
        except Exception as e:
            logger.error(f"❌ Исключение при обновлении squads: {e}")
            return False

    async def bulk_update_internal_squads(
        self, user_ids: list, active_internal_squads: list
    ) -> tuple[bool, int]:
        """
        POST /api/users/bulk/update-squads (до 500 id за запрос).
        Возвращает (успех, affectedRows из ответа или 0).
        """
        normalized_ids: list[int] = []
        for raw_id in user_ids:
            try:
                normalized_ids.append(int(raw_id))
            except (TypeError, ValueError):
                continue
        if not normalized_ids:
            return True, 0
        try:
            data = {
                "userIds": normalized_ids,
                "activeInternalSquads": active_internal_squads,
            }
            session = await self._get_session()
            async with session.post(
                f"{self.target_url}/api/users/bulk/update-squads",
                json=data,
                params=self.params,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as response:
                if response.status != 200:
                    err = (await response.text())[:500]
                    logger.error(f"bulk/update-squads HTTP {response.status}: {err}")
                    return False, 0
                try:
                    body = await response.json()
                except (aiohttp.ContentTypeError, ValueError):
                    logger.warning("bulk/update-squads: не JSON, считаем успехом")
                    return True, len(normalized_ids)
                resp = body.get("response") or {}
                affected = int(resp.get("affectedRows", 0))
                return True, affected
        except Exception as e:
            logger.error(f"bulk/update-squads: {e}")
            return False, 0

    async def get_all_panel(self):
        """
        Возвращает список всех пользователей из панели (объекты пользователей),
        у которых description == 'New user - without pay'.
        """
        lst_users = []
        try:
            users_all = []
            for i in range(200):  # максимум 100 страниц
                data = await self.list(1000 * i + 1)
                if data['response']['users']:
                    users_all.extend(data['response']['users'])
                else:
                    break
            logger.info(f'Всего юзеров в панели - {len(users_all)}')
            for user in users_all:
                lst_users.append(user)
        except Exception as e:
            logger.error(f"Ошибка при получении всех пользователей: {e}")
        return lst_users

    async def set_expiration_date(self, username: str, target_date: datetime, user_id: int):
        """
        Устанавливает точную дату окончания подписки для пользователя в панели.
        - Если пользователь не существует, создаёт его через addClient (с day=0).
        - Если target_date меньше текущего времени UTC, заменяет на текущее время + 1 минута.
        - Возвращает (успех, реальная_установленная_дата_UTC) или (False, None).
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        effective_date = target_date if target_date > now else now + datetime.timedelta(minutes=1)

        # Проверяем существование пользователя
        user_data = await self.get_user_by_username(username)
        if not user_data or 'response' not in user_data:
            # Пользователь отсутствует – создаём
            if not await self.addClient(0, username, user_id):
                logger.error(f"Не удалось создать пользователя {username} для установки даты")
                return False, None
            # После создания получаем данные заново
            user_data = await self.get_user_by_username(username)
            if not user_data or 'response' not in user_data:
                logger.error(f"Не удалось получить данные созданного пользователя {username}")
                return False, None

        user = self._panel_user_from_response(user_data)
        panel_user_id = self._panel_user_id(user)
        if not user or panel_user_id is None:
            logger.error(f"Некорректный ответ панели для {username}")
            return False, None

        # Формируем данные для обновления (сохраняем остальные поля)
        traffic_limit_bytes = user.get('trafficLimitBytes', 0)
        traffic_limit_strategy = user.get('trafficLimitStrategy', 'NO_RESET')
        status = 'ACTIVE'  # Активируем подписку
        raw_squads = user.get('activeInternalSquads', [])
        squads = [s['uuid'] if isinstance(s, dict) else s for s in raw_squads]

        data = {
            "id": panel_user_id,
            "expireAt": effective_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            "status": status,
            "trafficLimitBytes": traffic_limit_bytes,
            "trafficLimitStrategy": traffic_limit_strategy,
            "activeInternalSquads": squads
        }

        session = await self._get_session()
        try:
            async with session.patch(
                    f"{self.target_url}/api/users",
                    json=data,
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                if response.status == 200:
                    try:
                        resp_json = await response.json()
                        if resp_json.get('success', True):
                            logger.info(f"✅ Установлена дата {effective_date} для {username}")
                            return True, effective_date
                        else:
                            logger.error(f"Ошибка API при установке даты: {resp_json}")
                            return False, None
                    except:
                        # Нет JSON, но статус 200 – считаем успехом
                        logger.warning(f"Установка даты для {username} вернула 200 без JSON, считаем успешной")
                        return True, effective_date
                else:
                    error_text = await response.text() if response.content else "No body"
                    logger.error(f"Ошибка HTTP {response.status} при установке даты: {error_text}")
                    return False, None
        except Exception as e:
            logger.error(f"Исключение при установке даты для {username}: {e}")
            return False, None
