from common.deployment import require_profile
"""Independent scheduler SQL session; shared Order tables are read through Core."""
from flask import Flask
import os
from sqlalchemy import func, select
from order_service.storage import database_url
from task_scheduler.models import LeaderState, db


def create_storage_app(config=None):
    app = Flask(__name__)
    app.config.update(APP_ENV=os.environ.get("APP_ENV", "production"),
        SQLALCHEMY_TRACK_MODIFICATIONS=False, AUTO_CREATE_SCHEMA=False, MAX_CONTENT_LENGTH=64 * 1024,
        SQLALCHEMY_ENGINE_OPTIONS={"pool_pre_ping": True, "hide_parameters": True})
    if config:
        app.config.update(config)
    if not app.testing:require_profile(app.config["APP_ENV"])
    if not app.config.get("SQLALCHEMY_DATABASE_URI"):
        app.config["SQLALCHEMY_DATABASE_URI"] = database_url()
    db.init_app(app)
    if app.config["AUTO_CREATE_SCHEMA"]:
        with app.app_context():
            initialize_schema(testing=app.testing)
    return app


def initialize_schema(*, testing=False):
    if testing:
        from order_service.models import db as order_db
        order_db.metadata.create_all(bind=db.engine)
    db.create_all()
    if db.session.get(LeaderState, 1) is None:
        db.session.add(LeaderState(id=1, epoch=0))
        db.session.commit()


def database_now():
    if db.engine.dialect.name == "mysql":
        return db.session.execute(select(func.utc_timestamp(6))).scalar_one()
    from order_service.models import utc_now
    return utc_now()
