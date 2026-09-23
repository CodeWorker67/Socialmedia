import asyncio

import uvicorn
from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from bot import bot
from config import WEB_API_PORT
from utils.menu_photos import init_menu_photos
from config_bd.models import create_tables, engine
from config_bd.migrate_users_wl_fields import migrate as migrate_wl_fields
from config_bd.migrate_wl_traffic_meta import migrate as migrate_wl_traffic_meta
from config_bd.migrate_platega_recurent_payload import migrate as migrate_platega_recurent_payload
from payments import pay_stars, pay_cryptobot, pay_platega, pay_freekassa, pay_wl_traffic, pay_platega_recurrent
from sheduler.check_connect import check_connect
from sheduler.check_cryptobot import check_cryptobot_payments
from sheduler.check_online import check_online_daily
from sheduler.check_platega import check_platega, check_platega_card, check_platega_crypto
from sheduler.check_fk import check_fk
# from sheduler.check_wata_sbp import check_wata_sbp
# from sheduler.check_wata_card import check_wata_card
from handlers import (
    handlers_user,
    handlers_statistic,
    handlers_admin,
    handlers_one,
    handlers_broadcast,
    handlers_discount_push,
    handlers_contest_funnel,
    handlers_export,
    handlers_import,
    handlers_devices,
    handlers_wl_traffic,
)
from sheduler.time_mes import send_message_cron
from logging_config import logger
from sheduler.time_mes_not_sub import send_push_cron
from sheduler.check_wl_traffic import check_wl_traffic_cron
from sheduler.accumulate_wl_traffic import accumulate_wl_traffic_cron
from sheduler.credit_forever_wl_monthly import credit_forever_wl_monthly_cron
from wl_traffic.constants import WL_ACCUMULATE_HOUR, WL_ACCUMULATE_MINUTE
from sheduler.backup_db import send_db_backup_cron
from web_api import app as web_app


async def set_commands(bot: Bot):
    commands = [
        BotCommand(command='/start', description='Запустить бота'),
    ]
    await bot.set_my_commands(commands)


# Функция конфигурирования и запуска бота
async def main() -> None:
    await create_tables()
    await migrate_wl_fields()
    await migrate_wl_traffic_meta()
    await migrate_platega_recurent_payload()

    # Инициализация диспетчера
    dp: Dispatcher = Dispatcher()
    # Админ-команды (/export, /export_full, /partner, …) — до broadcast FSM,
    # иначе незавершённая рассылка перехватывает любой текст, включая /команды.
    dp.include_router(handlers_export.router)
    dp.include_router(handlers_admin.router)
    dp.include_router(handlers_statistic.router)
    dp.include_router(handlers_broadcast.router)
    dp.include_router(handlers_discount_push.router)
    dp.include_router(handlers_contest_funnel.router)
    dp.include_router(handlers_user.router)
    dp.include_router(handlers_wl_traffic.router)
    dp.include_router(handlers_devices.router)
    dp.include_router(handlers_import.router)
    # dp.include_router(pay_platega.router)
    # dp.include_router(pay_wata.router)
    dp.include_router(pay_freekassa.router)
    dp.include_router(pay_platega_recurrent.router)
    dp.include_router(pay_wl_traffic.router)
    dp.include_router(pay_stars.router)
    dp.include_router(pay_cryptobot.router)

    # Запуск шедулера
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(send_message_cron, trigger='interval', minutes=10, args=[bot], misfire_grace_time=120)
    scheduler.add_job(check_wl_traffic_cron, trigger='interval', minutes=30, args=[bot], misfire_grace_time=600)
    scheduler.add_job(
        accumulate_wl_traffic_cron,
        trigger='cron',
        hour=WL_ACCUMULATE_HOUR,
        minute=WL_ACCUMULATE_MINUTE,
        id='wl_traffic_accumulate',
        misfire_grace_time=600,
    )
    scheduler.add_job(
        credit_forever_wl_monthly_cron,
        trigger='cron',
        day=1,
        hour=0,
        minute=5,
        args=[bot],
        id='wl_forever_monthly',
        misfire_grace_time=3600,
    )
    scheduler.add_job(check_connect, trigger='interval', minutes=14, misfire_grace_time=60)
    # scheduler.add_job(check_platega, trigger='interval', minutes=1, misfire_grace_time=10)
    # scheduler.add_job(check_platega_card, trigger='interval', minutes=1, misfire_grace_time=10)
    # scheduler.add_job(check_platega_crypto, trigger='interval', minutes=1, misfire_grace_time=10)
    scheduler.add_job(check_fk, trigger='interval', minutes=1, misfire_grace_time=10)
    # scheduler.add_job(check_wata_sbp, trigger='interval', minutes=1, misfire_grace_time=10)
    # scheduler.add_job(check_wata_card, trigger='interval', minutes=1, misfire_grace_time=10)
    scheduler.add_job(check_cryptobot_payments, trigger='interval', minutes=1, misfire_grace_time=10)
    scheduler.add_job(send_push_cron, trigger='interval', minutes=30, misfire_grace_time=60)
    scheduler.add_job(check_online_daily, 'cron', hour=2, minute=55, id='daily_online_stats', misfire_grace_time=60)
    scheduler.add_job(
        send_db_backup_cron,
        trigger='interval',
        minutes=30,
        args=[bot],
        id='db_backup_checker',
        max_instances=1,
        misfire_grace_time=300,
    )
    scheduler.start()

    await set_commands(bot)
    await init_menu_photos(bot)

    uv_config = uvicorn.Config(web_app, host="0.0.0.0", port=WEB_API_PORT)
    server = uvicorn.Server(uv_config)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Bot start polling, web API on port {}.", WEB_API_PORT)
        bot_task = asyncio.create_task(dp.start_polling(bot))
        api_task = asyncio.create_task(server.serve())
        await asyncio.gather(bot_task, api_task)
    except asyncio.CancelledError:
        logger.error("Polling was cancelled. Cleaning up...")
    finally:
        server.should_exit = True
        await bot.session.close()
        await engine.dispose()
        logger.info("Bot session closed, DB pool disposed.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
