import random
from datetime import datetime, timezone, timedelta
from typing import Optional

from bot import sql, x3, bot
from config import ADMIN_IDS, CHECKER_ID
from keyboard import create_kb, STYLE_PRIMARY
from logging_config import logger
import asyncio
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command

from sheduler.check_connect import check_connect

router = Router()

_MSK = timezone(timedelta(hours=3))


def _msk_dt_str(dt: Optional[datetime]) -> str:
    if dt is None:
        return "Нет"
    if dt.tzinfo is None:
        aware = dt.replace(tzinfo=timezone.utc)
    else:
        aware = dt.astimezone(timezone.utc)
    return aware.astimezone(_MSK).strftime("%d-%m-%Y %H:%M МСК")


def _panel_sub_line(activ_result: dict) -> str:
    t = activ_result.get("time", "-")
    if t in (None, "", "-"):
        return "Нет"
    return str(t)


def _panel_usernames_from_row(row: tuple) -> tuple[str, str]:
    """Пара username в панели: обычная, вайт (Telegram ID и ID_white)."""
    tg = int(row[1])
    s = str(tg)
    return s, f"{s}_white"


def _split_long_text(text: str, limit: int = 3800) -> list[str]:
    if len(text) <= limit:
        return [text]
    parts: list[str] = []
    rest = text
    while rest:
        parts.append(rest[:limit])
        rest = rest[limit:]
    return parts


@router.message(F.video, F.from_user.id.in_(ADMIN_IDS))
async def get_video(message: Message):
    await message.answer(message.video.file_id)


@router.message(F.photo, F.from_user.id.in_(ADMIN_IDS))
async def get_photo(message: Message):
    await message.answer(message.photo[-1].file_id)


@router.message(Command(commands=['user']))
async def user_info(message: Message):

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        # Извлекаем аргументы команды
        args = message.text.split()

        if len(args) < 2:
            await message.answer("❌ Использование: /user <telegram_id>\nНапример: /user 123456789")
            return

        user_id = int(args[1].strip())

        # Проверяем, существует ли пользователь в БД
        user_data = await sql.get_user(user_id)

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {user_id} не найден в базе данных.")
            return
        text = []
        for i in range(len(user_data)):
            if isinstance(user_data[i], datetime):
                item = user_data[i].strftime('%Y-%m-%d %H:%M:%S')
                text.append(item)
            elif user_data[i] is None:
                text.append('None')
            else:
                text.append(str(user_data[i]))
        text = '\n'.join(text)
        await message.answer(text)
    except Exception as e:
        await message.answer(f'Ошибка при формировании сообщения: {str(e)}')


@router.message(Command(commands=['pay']))
async def pay_info_command(message: Message):
    """Сводка подписок (БД / панель) и успешные платежи пользователя."""
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer("❌ Использование: /pay <telegram_id>\nНапример: /pay 123456789")
        return

    try:
        target_id = int(args[1].strip())
    except ValueError:
        await message.answer("❌ ID должен быть числом.")
        return

    user_row = await sql.get_user(target_id)
    if not user_row:
        await message.answer(f"❌ Пользователь {target_id} не найден в базе данных.")
        return

    reg_un, white_un = _panel_usernames_from_row(user_row)
    sub_db = user_row[9]
    white_db = user_row[10]

    try:
        ar_reg, ar_white = await asyncio.gather(
            x3.activ(reg_un),
            x3.activ(white_un),
        )
    except Exception as e:
        logger.exception("/pay: панель")
        await message.answer(f"❌ Ошибка запроса к панели: {e}")
        return

    pay_rows = await sql.get_user_subscription_payment_report(target_id)
    pay_lines: list[str] = []
    for tc, kind, days_s in pay_rows:
        if tc.tzinfo is None:
            tc_aware = tc.replace(tzinfo=timezone.utc)
        else:
            tc_aware = tc.astimezone(timezone.utc)
        ts = tc_aware.astimezone(_MSK).strftime("%d-%m-%Y %H:%M МСК")
        pay_lines.append(f"• {ts} — {kind} — {days_s} дн.")

    body = (
        f"<b>/pay {target_id}</b>\n\n"
        f"Подписка обычная в БД бота — {_msk_dt_str(sub_db)}\n"
        f"Подписка обычная в панели — {_panel_sub_line(ar_reg)}\n"
        f"Подписка вайт в БД бота — {_msk_dt_str(white_db)}\n"
        f"Подписка вайт в панели — {_panel_sub_line(ar_white)}\n\n"
        f"<b>Платежи:</b>\n"
    )
    if pay_lines:
        body += "\n".join(pay_lines)
    else:
        body += "Нет"

    for chunk in _split_long_text(body):
        await message.answer(chunk)


@router.message(Command(commands=['sub']))
async def set_subscription_date(message: Message):
    """Установка subscription_end_date или white_subscription_end_date в БД и панели"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Эта команда доступна только администраторам.")
        return

    try:
        args = message.text.split()
        if len(args) < 3:
            await message.answer(
                "❌ Использование:\n"
                "  /sub <telegram_id> <дата_время>               – обновить обычную подписку\n"
                "  /sub <telegram_id> white <дата_время>         – обновить белую подписку\n"
                "Примеры:\n"
                "  /sub 123456789 2026-02-01 17:14:27\n"
                "  /sub 123456789 white 2026-02-01 17:14:27\n"
                "Формат даты: YYYY-MM-DD HH:MM:SS"
            )
            return

        user_id = int(args[1].strip())

        # Определяем тип и позицию даты
        if args[2].lower() == 'white':
            is_white = True
            date_str = " ".join(args[3:])
        else:
            is_white = False
            date_str = " ".join(args[2:])

        # Парсим дату
        date_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d.%m.%Y %H:%M:%S",
            "%d.%m.%Y %H:%M"
        ]
        target_date = None
        for fmt in date_formats:
            try:
                target_date = datetime.strptime(date_str, fmt)
                target_date = target_date.replace(tzinfo=timezone.utc)  # панель работает в UTC
                break
            except ValueError:
                continue
        if target_date is None:
            await message.answer(f"❌ Неверный формат даты: {date_str}")
            return

        # Проверяем наличие пользователя в БД
        user_data = await sql.get_user(user_id)
        if not user_data:
            await message.answer("⚠️ Пользователь не найден в БД.")
            return

        # Формируем username для панели
        username = str(user_id) + ('_white' if is_white else '')

        # Устанавливаем дату в панели
        success, actual_date = await x3.set_expiration_date(username, target_date, user_id)

        if not success or actual_date is None:
            await message.answer("❌ Не удалось установить дату в панели. Подробности в логах.")
            return

        if is_white:
            await sql.update_white_subscription_end_date(user_id, actual_date)
        else:
            await sql.update_subscription_end_date(user_id, actual_date)

        # Сообщаем результат
        await message.answer(
            f"✅ Дата подписки успешно установлена!\n\n"
            f"👤 Пользователь: {user_id}\n"
            f"📅 Целевая дата (UTC): {target_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📅 Установленная в панели дата (UTC): {actual_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📝 Тип: {'white' if is_white else 'обычная'}\n"
            f"💾 База данных обновлена."
        )

    except Exception as e:
        logger.error(f"Ошибка в команде /sub: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")


@router.message(Command(commands=['delete']))
async def delete_user_command(message: Message):
    """Удаление пользователя из БД по Telegram ID"""

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        # Извлекаем аргументы команды
        args = message.text.split()

        if len(args) < 2:
            await message.answer("❌ Использование: /delete <telegram_id>\nНапример: /delete 123456789")
            return

        user_id_to_delete = int(args[1].strip())

        # Проверяем, существует ли пользователь в БД
        user_data = await sql.get_user(user_id_to_delete)

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {user_id_to_delete} не найден в базе данных.")
            return

        # Получаем информацию о пользователе для уведомления
        user_info = {
            "user_id": user_data[1],  # user_id
            "ref": user_data[2],  # ref
            "in_panel": user_data[4],  # in_panel
            "in_chanel": user_data[7] if len(user_data) > 7 else False  # in_chanel
        }

        # УДАЛЯЕМ ПОЛЬЗОВАТЕЛЯ ИЗ БД
        deletion_success = await sql.delete_from_db(user_id_to_delete)

        if deletion_success:
            # Логируем действие
            logger.info(f"Администратор {message.from_user.id} удалил пользователя {user_id_to_delete} из БД")

            # Формируем отчет об удалении
            report_message = (
                f"✅ Пользователь успешно удалён из базы данных\n\n"
                f"📋 Информация об удалённом пользователе:\n"
                f"├ ID: {user_info['user_id']}\n"
                f"├ Реферер: {user_info['ref'] if user_info['ref'] else 'нет'}\n"
                f"└ Брал ключ: {'✅ да' if user_info['in_panel'] else '❌ нет'}\n"
                f"⚠️ Пользователь удалён только из базы данных бота.\n"
                f"   Подписка в панели управления (X3) остаётся активной.\n"
                f"   Чтобы удалить полностью, используйте команду /gift на 0 дней."
            )

            await message.answer(report_message)

        else:
            await message.answer(f"❌ Ошибка при удалении пользователя {user_id_to_delete}.\n"
                                 "Возможно, пользователь уже был удалён или произошла ошибка базы данных.")

    except ValueError:
        await message.answer("❌ Неверный формат Telegram ID.\n"
                             "Используйте только цифры, например: /delete 123456789")
    except Exception as e:
        logger.error(f"Ошибка в команде /delete: {e}")
        await message.answer(f"❌ Произошла ошибка при выполнении команды: {str(e)}")


@router.message(Command("online"))
async def check_online(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    users_x3 = await x3.get_all_users()

    active_telegram_ids = []
    for user in users_x3:
        if user['userTraffic']['firstConnectedAt']:
            connected_str = user['userTraffic']['onlineAt']
            try:
                connected_dt = datetime.fromisoformat(connected_str.replace('Z', '+00:00'))
                connected_date = connected_dt.date()
                if connected_date == datetime.now().date():
                    telegram_id = user.get('telegramId')
                    if telegram_id is not None:
                        active_telegram_ids.append(int(telegram_id))
            except (ValueError, TypeError):
                continue

    count_pay = 0
    count_trial = 0
    for tg_id in active_telegram_ids:
        user_data = await sql.get_user(tg_id)
        if user_data:
            if user_data[8]:
                count_pay += 1
            else:
                count_trial += 1
    await message.answer(
        f"Всего юзеров в панели: {len(users_x3)}\n"
        f"Юзеров, которые были онлайн сегодня: {len(active_telegram_ids)}\n"
        f"Юзеры с платной подпиской: {count_pay}\n"
        f"Юзеры на триале: {count_trial}"
    )


@router.message(Command("balance_panel"))
async def check_online(message: Message):
    squad_1 = ['494bf6ce-d62b-4929-a980-dfc14b8b5ddb']
    squad_2 = ['2e6f13b9-58a0-4f46-bd76-0d294f00ef18']
    success_count = 0
    fail_count = 0
    if message.from_user.id not in ADMIN_IDS:
        return
    users_x3 = await x3.get_all_users()
    for user in users_x3:
        await asyncio.sleep(0.3)
        random_squad = random.choice([squad_1, squad_2])
        username = user.get('username', '')
        if 'white' not in username and 'cascade-bridge-system' not in username:
            uuid = user.get('uuid')
            connect = user.get('firstConnectedAt')
            if uuid and connect:
                if await x3.update_user_squads(uuid, random_squad):
                    success_count += 1
                else:
                    fail_count += 1
    await message.answer(f"{len(users_x3)} - всего юзеров в панели\n{success_count + fail_count} - подключенных\n{success_count} - обновлено\n{fail_count} - ошибка")


@router.message(Command(commands=['sync_panel']))
async def sync_panel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer("🔄 Запускаю синхронизацию пользователей...")

    # 1. Получаем всех пользователей из панели и строим словарь {telegramId: user_data}
    users_panel = await x3.get_all_users()
    panel_dict = {}
    for user in users_panel:
        tg_id = user.get('telegramId')
        if tg_id is not None:
            panel_dict[tg_id] = user

    # 2. Получаем список пользователей, у которых is_pay_null=True и subscription_end_date=None
    users_for_sync = await sql.select_subscribed_not_in_chanel()

    # 3. Статистика
    updated = 0          # обновлено дат в БД
    added_to_panel = 0   # добавлено в панель
    not_found = 0        # не найдено в панели (остались в списке)

    # 4. Обрабатываем каждого пользователя из списка на синхронизацию
    if CHECKER_ID is not None:
        await bot.send_message(CHECKER_ID,
                               'Добрый день. Мы создали Вам личный кабинет и начислили 5 дней пробного '
                               'доступа.\nПерейдите по ссылке, нажав на кнопку 🌐 Подключить Ускоритель соцсетей',
                               reply_markup=create_kb(
                                   1,
                                   styles={'connect_vpn': STYLE_PRIMARY},
                                   connect_vpn='🌐 Подключить Ускоритель соцсетей',
                               ))

    for user_id in users_for_sync:
        # Проверяем, есть ли пользователь в панели
        if user_id in panel_dict:
            user_data = panel_dict[user_id]

            # Получаем expireAt и преобразуем в datetime
            expire_str = user_data.get('expireAt')
            if expire_str:
                try:
                    expire_dt = datetime.fromisoformat(expire_str.replace('Z', '+00:00'))
                except Exception as e:
                    logger.error(f"Ошибка парсинга expireAt для {user_id}: {e}")
                    continue

                await sql.update_subscription_end_date(user_id, expire_dt)
                updated += 1
                logger.info(f"Обновлена дата для {user_id} до {expire_dt}")
        else:
            user_id_str = str(user_id)
            result = await x3.addClient(5, user_id_str, user_id)
            if result:
                added_to_panel += 1
                logger.info(f"Добавлен в панель пользователь {user_id} (day=0)")
                await bot.send_message(user_id,
                                       'Добрый день. Мы создали Вам личный кабинет и начислили 5 дней пробного '
                                       'доступа.\nПерейдите по ссылке, нажав на кнопку 🌐 Подключить Ускоритель соцсетей',
                                       reply_markup=create_kb(
                                           1,
                                           styles={'connect_vpn': STYLE_PRIMARY},
                                           connect_vpn='🌐 Подключить Ускоритель соцсетей',
                                       ))
            else:
                not_found += 1
                logger.warning(f"Не удалось добавить в панель пользователя {user_id}")

    # 5. Итоговый отчёт
    report = (
        f"✅ Синхронизация завершена.\n"
        f"📊 Всего в панели: {len(users_panel)}\n"
        f"📋 Ожидало синхронизации: {len(users_for_sync)}\n"
        f"🔄 Обновлено дат в БД: {updated}\n"
        f"➕ Добавлено в панель (day=5): {added_to_panel}\n"
        f"❌ Не удалось добавить (ошибки): {not_found}"
    )
    await message.answer(report)
    logger.info(report)


@router.message(Command(commands=['shortuuid_export']))
async def shortuuid_export(message: Message):
    """Синхронизация shortUuid из панели в поля subscribtion / white_subscription в БД."""
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer("🔄 Загружаю пользователей панели и записываю shortUuid в БД...")

    try:
        panel_users = await x3.get_all_users()
    except Exception as e:
        logger.error(f"shortuuid_export: панель: {e}")
        await message.answer(f"❌ Ошибка при запросе панели: {e}")
        return

    updated_sub = 0
    updated_white = 0
    skip_no_db = 0
    skip_no_tg = 0
    skip_no_short = 0
    errors = 0

    for user in panel_users:
        tg_id = user.get("telegramId")
        username = user.get("username") or ""
        if tg_id is None:
            if username.isdigit():
                tg_id = int(username)
            else:
                skip_no_tg += 1
                continue
        else:
            tg_id = int(tg_id)

        short_uuid = user.get("shortUuid")
        if not short_uuid:
            skip_no_short += 1
            continue

        db_user = await sql.get_user(tg_id)
        if not db_user:
            skip_no_db += 1
            continue

        is_white = "white" in username
        try:
            if is_white:
                await sql.update_white_subscription(tg_id, short_uuid)
                updated_white += 1
            else:
                await sql.update_subscribtion(tg_id, short_uuid)
                updated_sub += 1
            logger.success(f"shortuuid_export user {tg_id}: {short_uuid}")
        except Exception as e:
            errors += 1
            logger.error(f"shortuuid_export user {tg_id}: {e}")

    report = (
        f"✅ Готово.\n"
        f"📊 В панели записей: {len(panel_users)}\n"
        f"📝 subscribtion обновлено: {updated_sub}\n"
        f"📝 white_subscription обновлено: {updated_white}\n"
        f"⏭ без telegramId/username: {skip_no_tg}\n"
        f"⏭ без shortUuid: {skip_no_short}\n"
        f"⏭ нет в БД: {skip_no_db}\n"
        f"❌ ошибок записи: {errors}"
    )
    await message.answer(report)
    logger.info(report)


@router.message(Command(commands=['check_users']))
async def check_users_command(message: Message):
    """Проверка соответствия дат окончания подписки у оплаченных пользователей (has_discount=True)"""
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer("🔄 Начинаю проверку пользователей с оплатами...")

    try:
        # 1. Получаем список оплаченных пользователей из БД
        users_with_discount = await sql.get_users_with_payment()
        total = len(users_with_discount)
        if total == 0:
            await message.answer("❌ Нет пользователей с оплатами.")
            return

        # 2. Получаем всех пользователей из панели (один запрос)
        panel_users = await x3.get_all_users()
        logger.info(f"Загружено {len(panel_users)} пользователей из панели")

        # 3. Строим словарь для быстрого поиска по telegramId и username
        panel_by_telegram = {}      # ключ: telegramId (int)
        panel_by_username = {}      # ключ: username (str)

        for user in panel_users:
            tg_id = user.get('telegramId')
            username = user.get('username')
            if tg_id is not None:
                panel_by_telegram[int(tg_id)] = user
            elif username:
                panel_by_username[username] = user

        # 4. Проходим по всем оплаченным пользователям и ищем их в панели
        mismatched = []      # кортежи (user_id, db_date, panel_date) для расхождений >=3ч
        not_found_in_panel = []  # пользователи, отсутствующие в панели
        processed = 0

        for user_id in users_with_discount:
            processed += 1
            if processed % 10 == 0:
                logger.info(f"Проверено {processed}/{total}")

            # Пытаемся найти пользователя в панели
            panel_user = panel_by_telegram.get(user_id)
            if panel_user is None:
                panel_user = panel_by_username.get(str(user_id))

            if panel_user is None:
                not_found_in_panel.append(user_id)
                continue

            expire_str = panel_user.get('expireAt')
            if not expire_str:
                # нет даты в панели – считаем расхождением (panel_date = None)
                db_expire = await sql.get_subscription_end_date(user_id)
                mismatched.append((user_id, db_expire, None))
                continue

            try:
                panel_expire = datetime.fromisoformat(expire_str.replace('Z', '+00:00'))
            except Exception:
                # не удалось распарсить дату панели
                db_expire = await sql.get_subscription_end_date(user_id)
                mismatched.append((user_id, db_expire, None))
                continue

            # Получаем дату из БД (обычная подписка)
            db_expire = await sql.get_subscription_end_date(user_id)
            panel_naive = panel_expire.replace(tzinfo=None)

            if db_expire is None:
                # нет даты в БД
                mismatched.append((user_id, None, panel_naive))
                continue

            db_naive = db_expire.replace(tzinfo=None)
            diff_hours = abs((panel_naive - db_naive).total_seconds()) / 3600

            if diff_hours >= 6:
                mismatched.append((user_id, db_naive, panel_naive))

        # 5. Формируем отчёт
        report_lines = []
        report_lines.append(f"📊 Результаты проверки:\n")
        report_lines.append(f"👥 Всего проверено: {total}")
        report_lines.append(f"❌ Расхождений в датах (>=6ч): {len(mismatched)}")
        report_lines.append(f"🔍 Не найдены в панели: {len(not_found_in_panel)}")

        # Если есть расхождения и их количество не превышает лимит для прямого вывода
        if mismatched or not_found_in_panel:
            if len(mismatched) <= 50 and len(not_found_in_panel) <= 50:
                if mismatched:
                    report_lines.append("\n🆔 Расхождения (команды для синхронизации):")
                    for uid, db_dt, panel_dt in mismatched:
                        db_str = db_dt.strftime('%Y-%m-%d %H:%M:%S') if db_dt else 'None'
                        panel_str = panel_dt.strftime('%Y-%m-%d %H:%M:%S') if panel_dt else 'None'
                        report_lines.append(f"/sub {uid} {db_str} /sub {uid} {panel_str}")
                if not_found_in_panel:
                    report_lines.append("\n🆔 Не найдены в панели:")
                    report_lines.extend(str(uid) for uid in not_found_in_panel)
                await message.answer("\n".join(report_lines))
            else:
                # Если много расхождений – отправляем файлом
                import io
                text_io = io.StringIO()
                text_io.write("user_id\tdb_date\tpanel_date\n")
                for uid, db_dt, panel_dt in mismatched:
                    db_str = db_dt.strftime('%Y-%m-%d %H:%M:%S') if db_dt else 'None'
                    panel_str = panel_dt.strftime('%Y-%m-%d %H:%M:%S') if panel_dt else 'None'
                    text_io.write(f"/sub {uid} {db_str} /sub {uid} {panel_str}\n")
                for uid in not_found_in_panel:
                    text_io.write(f"{uid}\tnot_found\n")
                text_io.seek(0)
                from aiogram.types import BufferedInputFile
                file_data = BufferedInputFile(text_io.getvalue().encode(), filename="check_users_report.txt")
                await message.answer_document(
                    document=file_data,
                    caption="\n".join(report_lines[:5])
                )
        else:
            await message.answer("✅ Все оплаченные пользователи синхронизированы (разница менее 3 часов).")

    except Exception as e:
        logger.exception("Ошибка в /check_users")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.message(Command(commands=['send_gift']))
async def send_gift_command(message: Message):
    """Отправляет подарок (3 дня подписки) пользователям, созданным 16 или 17 марта 2026,
    у которых in_panel=True, is_connect=False, is_delete=False."""
    if CHECKER_ID is None or message.from_user.id != CHECKER_ID:
        return

    await message.answer("🔄 Начинаю отправку подарков...")

    # Целевые даты
    target_dates = (datetime(2026, 3, 16), datetime(2026, 3, 17))

    # Получаем всех пользователей из БД (можно фильтровать на стороне Python, т.к. запрос сложный)
    all_users = await sql.get_all_users()  # список объектов Users

    # Фильтруем вручную
    candidates = [CHECKER_ID]
    for user in all_users:
        if user.is_delete:
            continue
        if not user.in_panel:
            continue
        if user.is_connect:
            continue
        if user.create_user.date() not in [d.date() for d in target_dates]:
            continue
        candidates.append(user.user_id)

    if not candidates:
        await message.answer("❌ Нет пользователей, удовлетворяющих условиям.")
        return
    else:
        await message.answer(f"Всего {len(candidates)} пользователей, удовлетворяющих условиям.")

    success_count = 0
    fail_count = 0

    # Текст сообщения
    gift_text = '''
🥵 Это была DDoS-атака!

Друзья, простите за временные неудобства. Сервис работает в штатном режиме.

Мы столкнулись с мощной DDoS-атакой, если у вас <b>не открывался личный кабинет — проблема уже решена.</b>

🔥 Мы начислили вам <b>дополнительные 5 дней</b> к подписке, чтобы вы могли оценить удобство Ускорителя соцсетей.

📱 Не можете настроить?
Если вы никак не могли разобраться с импортом конфигов — <b>смотрите видеоинструкцию</b>! Там всё разложено по полочкам.

🌐 Осталось только нажать кнопку "🌐 Подключить Ускоритель соцсетей" — и вы в деле.
            '''

    for user_id in candidates[83:]:
        try:
            # Отправляем сообщение
            await bot.send_message(user_id,
                                   gift_text,
                                   reply_markup=create_kb(
                                       1,
                                       styles={
                                           'video_faq': STYLE_PRIMARY,
                                           'connect_vpn': STYLE_PRIMARY,
                                       },
                                       video_faq='🎥 Видеоинструкция',
                                       connect_vpn='🌐 Подключить Ускоритель соцсетей',
                                   ))
            # Добавляем 3 дня подписки
            result = await x3.updateClient(5, str(user_id), user_id)
            if result:
                success_count += 1
                logger.info(f"Подарок отправлен пользователю {user_id}")
            else:
                fail_count += 1
                logger.error(f"Не удалось обновить подписку для {user_id}")
            await asyncio.sleep(0.05)  # небольшая задержка
        except Exception as e:
            fail_count += 1
            logger.error(f"Ошибка при обработке {user_id}: {e}")

    await message.answer(
        f"✅ Рассылка подарков завершена.\n"
        f"👥 Найдено: {len(candidates)}\n"
        f"✅ Успешно: {success_count}\n"
        f"❌ Ошибок: {fail_count}"
    )


@router.message(Command(commands=['send_push']))
async def send_push_command(message: Message):
    """Отправляет информационное сообщение пользователям, созданным до 16 марта 2026,
    с активной подпиской (in_panel=True, subscription_end_date > now, is_delete=False)."""
    if CHECKER_ID is None or message.from_user.id != CHECKER_ID:
        return

    await message.answer("🔄 Начинаю отправку push-уведомления...")

    # Получаем всех пользователей
    all_users = await sql.get_all_users()

    # Фильтруем
    candidates = [CHECKER_ID]
    for user in all_users:
        if user.is_delete:
            continue
        if not user.in_panel:
            continue
        if user.subscription_end_date:
            continue
        candidates.append(user.user_id)

    if not candidates:
        await message.answer("❌ Нет пользователей, удовлетворяющих условиям.")
        return
    else:
        await message.answer(f"Всего {len(candidates)} пользователей, удовлетворяющих условиям.")

    push_text = '''
🥵 Это была DDoS-атака!

Друзья, простите за временные неудобства. Сервис работает в штатном режиме.
Мы столкнулись с мощной DDoS-атакой, если у вас <b>не открывался личный кабинет — проблема уже решена.</b>

📱 Не можете настроить?
Если вы никак не могли разобраться с импортом конфигов — <b>смотрите видеоинструкцию</b>! Там всё разложено по полочкам.

🌐 Осталось только нажать кнопку "🌐 Подключить Ускоритель соцсетей" — и вы снова в деле.
    '''

    success_count = 0
    fail_count = 0

    for user_id in candidates:
        try:
            user_data = await x3.get_user_by_username(str(user_id))
            if user_data:
                logger.success(f'{user_id} уже в панели')
                raw = user_data['response']
                user = raw[0] if isinstance(raw, list) else raw
                if not isinstance(user, dict):
                    logger.error(f"send_push: неверный формат response для {user_id}")
                    continue

                expire_str = user.get('expireAt')
                if expire_str:
                    try:
                        expire_dt = datetime.fromisoformat(expire_str.replace('Z', '+00:00'))
                        await sql.update_subscription_end_date(user_id, expire_dt)
                    except Exception as e:
                        logger.error(f"send_push: парсинг expireAt для {user_id}: {e}")

                short_uuid = user.get('shortUuid')
                if short_uuid:
                    username_panel = user.get('username') or ''
                    is_white = 'white' in username_panel
                    try:
                        if is_white:
                            await sql.update_white_subscription(user_id, short_uuid)
                        else:
                            await sql.update_subscribtion(user_id, short_uuid)
                    except Exception as e:
                        logger.error(f"send_push: запись shortUuid для {user_id}: {e}")

                continue
            await x3.addClient(5, str(user_id), int(user_id))
            await bot.send_message(user_id,
                                   push_text,
                                   reply_markup=create_kb(
                                       1,
                                       styles={
                                           'video_faq': STYLE_PRIMARY,
                                           'connect_vpn': STYLE_PRIMARY,
                                       },
                                       video_faq='🎥 Видеоинструкция',
                                       connect_vpn='🌐 Подключить Ускоритель соцсетей',
                                   ))
            success_count += 1
            logger.info(f"Push отправлен пользователю {user_id}")
            await asyncio.sleep(0.05)
        except Exception as e:
            fail_count += 1
            logger.error(f"Ошибка отправки для {user_id}: {e}")

    await message.answer(
        f"✅ Рассылка завершена.\n"
        f"👥 Найдено: {len(candidates)}\n"
        f"✅ Успешно: {success_count}\n"
        f"❌ Ошибок: {fail_count}"
    )


_NEW_PANEL_SQUAD_1 = "xxx"
_NEW_PANEL_SQUAD_2 = "yyy"
_NEW_PANEL_WHITE_SQUAD = "zzz"
_NEW_PANEL_BULK_BATCH = 500


async def _new_panel_bulk_uuids(uuids: list, squad: str) -> tuple[bool, int]:
    """Разбивает UUID на батчи и вызывает bulk_update_internal_squads."""
    total_affected = 0
    all_ok = True
    for off in range(0, len(uuids), _NEW_PANEL_BULK_BATCH):
        batch = uuids[off : off + _NEW_PANEL_BULK_BATCH]
        ok, aff = await x3.bulk_update_internal_squads(batch, [squad])
        total_affected += aff
        if not ok:
            all_ok = False
        await asyncio.sleep(0.15)
    return all_ok, total_affected


@router.message(Command(commands=["new_panel"]))
async def new_panel_command(message: Message):
    """Массовое обновление internal squads: white → white_squad, цифровые username → squad_1/squad_2."""
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        users = await x3.get_all_panel()
        total_panel = len(users)

        casual_list: list[dict] = []
        white_list: list[dict] = []
        skipped_no_username = 0
        skipped_other = 0

        for u in users:
            un = u.get("username")
            if un is None or str(un).strip() == "":
                skipped_no_username += 1
                continue
            s = str(un)
            if "white" in s:
                white_list.append(u)
            elif s.isdigit():
                casual_list.append(u)
            else:
                skipped_other += 1

        white_uuids = [str(u["uuid"]) for u in white_list if u.get("uuid")]
        white_no_uuid = len(white_list) - len(white_uuids)
        casual_by_squad: dict[str, list[str]] = {_NEW_PANEL_SQUAD_1: [], _NEW_PANEL_SQUAD_2: []}
        casual_no_uuid = 0
        for u in casual_list:
            uid = u.get("uuid")
            if not uid:
                casual_no_uuid += 1
                continue
            sq = random.choice([_NEW_PANEL_SQUAD_1, _NEW_PANEL_SQUAD_2])
            casual_by_squad[sq].append(str(uid))

        classified = len(casual_list) + len(white_list)
        bulk_total = len(white_uuids) + len(casual_by_squad[_NEW_PANEL_SQUAD_1]) + len(
            casual_by_squad[_NEW_PANEL_SQUAD_2]
        )
        await message.answer(
            f"📋 /new_panel\n"
            f"В панели записей: {total_panel}\n"
            f"По username: обычные — {len(casual_list)}, white — {len(white_list)} "
            f"(всего классифицировано {classified})\n"
            f"К bulk-обновлению (есть uuid): {bulk_total}\n"
            f"Пропуск: без username — {skipped_no_username}, иной формат username — {skipped_other}\n"
            f"🔄 Начинаю обновление сквадов…"
        )

        white_ok, white_aff = await _new_panel_bulk_uuids(white_uuids, _NEW_PANEL_WHITE_SQUAD)

        casual_ok = True
        casual_aff = 0
        n_s1 = len(casual_by_squad[_NEW_PANEL_SQUAD_1])
        n_s2 = len(casual_by_squad[_NEW_PANEL_SQUAD_2])
        for sq, uuids in casual_by_squad.items():
            if not uuids:
                continue
            ok, aff = await _new_panel_bulk_uuids(uuids, sq)
            casual_aff += aff
            if not ok:
                casual_ok = False

        report = (
            f"✅ /new_panel — отчёт\n"
            f"White: UUID {len(white_uuids)}, affected Σ={white_aff}, "
            f"{'ok' if white_ok else 'были ошибки (см. лог)'}\n"
            f"Casual: squad_1 — {n_s1} юз., squad_2 — {n_s2} юз. "
            f"(random_choice между ними), affected Σ={casual_aff}, "
            f"{'ok' if casual_ok else 'были ошибки (см. лог)'}\n"
        )
        if white_no_uuid:
            report += f"White без uuid в панели: {white_no_uuid}\n"
        if casual_no_uuid:
            report += f"Casual без uuid в панели: {casual_no_uuid}\n"
        await message.answer(report)
        logger.info(
            f"Админ {message.from_user.id} /new_panel: white={len(white_uuids)} casual={len(casual_list)} "
            f"white_ok={white_ok} casual_ok={casual_ok}"
        )
    except Exception as e:
        logger.exception("Ошибка в /new_panel")
        await message.answer(f"❌ Ошибка: {str(e)}")
