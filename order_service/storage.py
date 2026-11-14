from common.deployment import require_profile
"""Local SQL setup with protected URL construction and an explicit schema step."""
import os
from flask import Flask
from sqlalchemy.engine import URL
from order_service.models import db


def database_url(environ=None):
    env = os.environ if environ is None else environ
    password = env.get("DB_PASSWORD")
    if not password:
        raise ValueError("DB_PASSWORD must be set in the process environment.")
    try:
        port = int(env.get("DB_PORT", "3306"))
    except ValueError:
        raise ValueError("DB_PORT must be an integer between 1 and 65535.") from None
    if not 1 <= port <= 65535:
        raise ValueError("DB_PORT must be an integer between 1 and 65535.")
    host = env.get("DB_PRIVATE_IP", "127.0.0.1")
    if env.get("APP_ENV") == "local" and host not in {"mysql", "localhost", "127.0.0.1"}:
        raise ValueError("Local Order Service requires a local database host.")
    return URL.create("mysql+pymysql", username=env.get("DB_USER", "customer_service_user"),
                      password=password, host=host, port=port, database=env.get("DB_NAME", "orders_db"))


def create_storage_app(config=None):
    app = Flask(__name__)
    app.config.update(APP_ENV=os.environ.get("APP_ENV", "production"),
                      SQLALCHEMY_TRACK_MODIFICATIONS=False, AUTO_CREATE_SCHEMA=False,
                      SQLALCHEMY_ENGINE_OPTIONS={"pool_pre_ping": True, "hide_parameters": True},
                      MAX_CONTENT_LENGTH=32 * 1024)
    if config:
        app.config.update(config)
    if not app.testing:require_profile(app.config["APP_ENV"])
    if not app.config.get("SQLALCHEMY_DATABASE_URI"):
        app.config["SQLALCHEMY_DATABASE_URI"] = database_url()
    db.init_app(app)
    if app.config["AUTO_CREATE_SCHEMA"]:
        with app.app_context():
            from order_service.models import Order, Idempotency, OrderEvent, Outbox
            if app.testing:
                db.create_all()
            else:
                db.metadata.create_all(bind=db.engine, tables=[m.__table__ for m in (Order, Idempotency, OrderEvent, Outbox)])
    return app
