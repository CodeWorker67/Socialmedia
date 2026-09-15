import logging

from sqlalchemy import Column, Integer, String, DateTime, Boolean, BigInteger, Date, Float, event
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime

DB_URL = "sqlite+aiosqlite:///config_bd/socialvpn.db"
engine = create_async_engine(DB_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase, AsyncAttrs):
    pass


class Users(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, unique=True, nullable=False)
    ref = Column(String(100), nullable=True)
    is_delete = Column(Boolean, default=False)
    in_panel = Column(Boolean, default=False)
    is_connect = Column(Boolean, default=False)
    create_user = Column(DateTime, default=datetime.now)
    in_chanel = Column(Boolean, default=False)
    reserve_field = Column(Boolean, default=False)
    subscription_end_date = Column(DateTime, nullable=True)
    last_notification_date = Column(Date, nullable=True)
    last_broadcast_status = Column(String(100), nullable=True)
    last_broadcast_date = Column(DateTime, nullable=True)
    stamp = Column(String(100), nullable=False)
    ttclid = Column(String(100), nullable=True)
    subscribtion = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True, unique=True)
    password = Column(String(255), nullable=True)
    password_hash = Column(String(255), nullable=True)
    linked_telegram_id = Column(BigInteger, nullable=True)
    activation_pass = Column(String(255), nullable=True)
    field_str_1 = Column(String(255), nullable=True)
    field_str_2 = Column(String(255), nullable=True)
    field_str_3 = Column(String(255), nullable=True)
    field_bool_1 = Column(Boolean, default=False)
    field_bool_2 = Column(Boolean, default=False)
    field_bool_3 = Column(Boolean, default=False)
    partner = Column(String(100), nullable=True)
    partner_balance = Column(Integer, default=0)
    partner_pay = Column(Integer, default=0)
    partner_flag = Column(Boolean, default=False)
    trafic_wl = Column(Float, default=0.0)
    limit_wl = Column(Float, default=0.0)


class WlTrafficMeta(Base):
    """Глобальное состояние учёта WL-трафика (одна строка id=1)."""
    __tablename__ = 'wl_traffic_meta'

    id = Column(Integer, primary_key=True, default=1)
    last_closed_date = Column(Date, nullable=True)


class Gifts(Base):
    __tablename__ = 'gifts'

    gift_id = Column(String(36), primary_key=True)
    giver_id = Column(BigInteger, nullable=False)
    duration = Column(Integer, nullable=False)
    recepient_id = Column(BigInteger, nullable=True)
    white_flag = Column(Boolean, default=False)
    flag = Column(Boolean, default=False)


class Payments(Base):
    __tablename__ = 'payments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    amount = Column(Integer, nullable=False)
    time_created = Column(DateTime, default=datetime.now)
    is_gift = Column(Boolean, default=False)
    status = Column(String, nullable=True)
    transaction_id = Column(String, nullable=True)
    payload = Column(String, nullable=True)


class PaymentsCards(Base):
    __tablename__ = 'payments_cards'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    amount = Column(Integer, nullable=False)
    time_created = Column(DateTime, default=datetime.now)
    is_gift = Column(Boolean, default=False)
    status = Column(String, nullable=True)
    transaction_id = Column(String, nullable=True)
    payload = Column(String, nullable=True)


class PaymentsPlategaCrypto(Base):
    __tablename__ = 'payments_platega_crypto'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    amount = Column(Integer, nullable=False)
    time_created = Column(DateTime, default=datetime.now)
    is_gift = Column(Boolean, default=False)
    status = Column(String, nullable=True)
    transaction_id = Column(String, nullable=True)
    payload = Column(String, nullable=True)


class PaymentsWataSBP(Base):
    __tablename__ = 'payments_wata_sbp'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    amount = Column(Integer, nullable=False)
    time_created = Column(DateTime, default=datetime.now)
    is_gift = Column(Boolean, default=False)
    status = Column(String, nullable=True)
    transaction_id = Column(String, nullable=True)
    payload = Column(String, nullable=True)


class PaymentsWataCard(Base):
    __tablename__ = 'payments_wata_card'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    amount = Column(Integer, nullable=False)
    time_created = Column(DateTime, default=datetime.now)
    is_gift = Column(Boolean, default=False)
    status = Column(String, nullable=True)
    transaction_id = Column(String, nullable=True)
    payload = Column(String, nullable=True)


class PaymentsFkSBP(Base):
    __tablename__ = 'payments_fk_sbp'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    amount = Column(Integer, nullable=False)
    time_created = Column(DateTime, default=datetime.now)
    is_gift = Column(Boolean, default=False)
    status = Column(String, nullable=True)
    transaction_id = Column(String, nullable=True)
    fk_order_id = Column(Integer, nullable=True)
    payload = Column(String, nullable=True)
    nonce = Column(BigInteger, nullable=False)
    signature = Column(String, nullable=True)
    method = Column(String, nullable=False, default='fk_qr_card')


class PaymentsStars(Base):
    __tablename__ = 'payments_stars'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    amount = Column(Integer, nullable=False)
    time_created = Column(DateTime, default=datetime.now)
    is_gift = Column(Boolean, default=False)
    status = Column(String, default='confirmed')
    payload = Column(String, nullable=True)


class PaymentsCryptobot(Base):
    __tablename__ = 'payments_cryptobot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String(10), nullable=False)
    time_created = Column(DateTime, default=datetime.now)
    is_gift = Column(Boolean, default=False)
    status = Column(String, default='pending')
    invoice_id = Column(String, nullable=True)
    payload = Column(String, nullable=True)


class WhiteCounter(Base):
    __tablename__ = 'white_counter'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    time_created = Column(DateTime, default=datetime.now)


class LinkingCodes(Base):
    __tablename__ = 'linking_codes'

    code_id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(32), nullable=False, unique=True)
    user_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    expires_at = Column(DateTime, nullable=False)


class PasswordResetCodes(Base):
    __tablename__ = 'password_reset_codes'

    pass_id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), nullable=False)
    code = Column(String(16), nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    expires_at = Column(DateTime, nullable=False)


class Online(Base):
    __tablename__ = 'online'

    online_id = Column(Integer, primary_key=True, autoincrement=True)
    online_date = Column(DateTime, default=datetime.now, nullable=False)
    users_panel = Column(Integer, nullable=False)
    users_active = Column(Integer, nullable=False)
    users_pay = Column(Integer, nullable=False)
    users_trial = Column(Integer, nullable=False)
    users_subscribed = Column(Integer, nullable=True)


class PlategaAutopaySubscription(Base):
    """Активные/ожидающие рекуррентные СБП-подписки Platega у клиентов."""
    __tablename__ = 'platega_autopay_subscriptions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    subscription_id = Column(String(64), unique=True, nullable=False)
    duration = Column(String(16), nullable=False)
    amount = Column(Integer, nullable=False)
    status = Column(String(32), nullable=False, default='pending')
    next_charge_at = Column(DateTime, nullable=True)
    time_created = Column(DateTime, default=datetime.now)
    time_cancelled = Column(DateTime, nullable=True)
    cancel_reason = Column(String(64), nullable=True)
    payload = Column(String, nullable=True)
    white = Column(Boolean, default=False)
    source = Column(String(32), nullable=True)


class PlategaRecurent(Base):
    """Каждое списание по рекуррентной подписке (webhook Platega)."""
    __tablename__ = 'platega_recurent'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    subscription_id = Column(String(64), nullable=False, index=True)
    transaction_id = Column(String(64), unique=True, nullable=False)
    amount = Column(Integer, nullable=False)
    currency = Column(String(8), nullable=False, default='RUB')
    status = Column(String(32), nullable=False)
    payment_method = Column(Integer, nullable=True)
    payload = Column(String, nullable=True)
    next_charge_at = Column(DateTime, nullable=True)
    time_created = Column(DateTime, default=datetime.now)
    processed = Column(Boolean, default=False)


# Функция для создания таблиц (запустить один раз)
async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)