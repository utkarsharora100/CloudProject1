from common.observability import install as install_observability
from common.deployment import require_profile
"""Local-demo Customer API. Public authentication is a later deployment gate."""
import os
from flask import Flask, jsonify, request
from sqlalchemy import select
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from werkzeug.exceptions import BadRequest, Conflict, HTTPException, NotFound
from customer_service.models import Address, Customer, db
from customer_service.validation import validate


MAX_ADDRESSES = 20


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
        raise ValueError("The local Customer API requires a local database host.")
    return URL.create("mysql+pymysql", username=env.get("DB_USER", "customer_service_user"),
                      password=password, host=host, port=port,
                      database=env.get("DB_NAME", "orders_db"))


def create_app(config=None):
    app = Flask(__name__)
    local = os.environ.get("APP_ENV") == "local"
    app.config.update(APP_ENV=os.environ.get("APP_ENV", "production"),
                      SQLALCHEMY_TRACK_MODIFICATIONS=False,
                      SQLALCHEMY_ENGINE_OPTIONS={"pool_pre_ping": True, "hide_parameters": True},
                      MAX_CONTENT_LENGTH=32 * 1024, AUTO_CREATE_SCHEMA=local)
    if config:
        app.config.update(config)
    if not app.testing:require_profile(app.config["APP_ENV"])
    if not app.config.get("SQLALCHEMY_DATABASE_URI"):
        app.config["SQLALCHEMY_DATABASE_URI"] = database_url()
    db.init_app(app)
    if app.config["AUTO_CREATE_SCHEMA"]:
        with app.app_context():
            # Only these two new tables are in this service's metadata.
            db.create_all()

    def rollback():
        try:
            db.session.rollback()
        except SQLAlchemyError:
            db.session.remove()

    @app.after_request
    def privacy_headers(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.errorhandler(HTTPException)
    def http_error(error):
        response = error.get_response()
        response.data = app.json.dumps({"error": error.description})
        response.content_type = "application/json"
        return response

    @app.errorhandler(IntegrityError)
    def integrity_error(error):
        rollback()
        return jsonify({"error": "Customer data conflicts with an existing record."}), 409

    @app.errorhandler(SQLAlchemyError)
    def database_error(error):
        rollback()
        # Never log the exception, SQL parameters, connection URL or submitted data.
        app.logger.error("Customer database operation failed.")
        return jsonify({"error": "Customer data is temporarily unavailable."}), 503

    @app.errorhandler(Exception)
    def unexpected_error(error):
        rollback()
        app.logger.error("Unexpected Customer Service operation failure.")
        return jsonify({"error": "The customer operation could not be completed."}), 500

    def customer_record(customer_id, *, lock=False):
        if not 1 <= customer_id <= 2147483647:
            raise NotFound("Customer not found.")
        query = select(Customer).where(Customer.id == customer_id)
        if lock:
            query = query.with_for_update()
        customer = db.session.execute(query).scalar_one_or_none()
        if customer is None:
            raise NotFound("Customer not found.")
        return customer

    def address_record(customer_id, address_id):
        if not 1 <= address_id <= 2147483647:
            raise NotFound("Address not found for this customer.")
        address = db.session.execute(select(Address).where(
            Address.id == address_id, Address.customer_id == customer_id)).scalar_one_or_none()
        if address is None:
            raise NotFound("Address not found for this customer.")
        return address

    def choose_default(customer_id, address):
        # All address mutations hold the parent Customer row lock. Serializing on
        # it prevents concurrent requests creating multiple default addresses.
        Address.query.filter_by(customer_id=customer_id).update(
            {"is_default": False}, synchronize_session="fetch")
        address.is_default = True

    @app.get("/health")
    def health():
        db.session.execute(select(Customer.id).limit(1)).first()
        db.session.execute(select(Address.id).limit(1)).first()
        return jsonify({"status": "healthy", "service": "customer-service"})

    @app.post("/customers")
    def create_customer():
        data = validate(request.get_json(), profile=True)
        customer = Customer(**data)
        db.session.add(customer)
        db.session.commit()
        response = jsonify({"customer": customer.as_dict()})
        response.status_code = 201
        response.headers["Location"] = f"/customers/{customer.id}"
        return response

    @app.get("/customers/<int:customer_id>")
    def get_customer(customer_id):
        return jsonify({"customer": customer_record(customer_id).as_dict()})

    @app.patch("/customers/<int:customer_id>")
    def update_customer(customer_id):
        data = validate(request.get_json(), profile=True, partial=True)
        customer = customer_record(customer_id, lock=True)
        for field, value in data.items():
            setattr(customer, field, value)
        db.session.commit()
        return jsonify({"customer": customer.as_dict()})

    @app.get("/customers/<int:customer_id>/addresses")
    def list_addresses(customer_id):
        customer_record(customer_id)
        addresses = db.session.execute(select(Address).where(
            Address.customer_id == customer_id).order_by(Address.id)).scalars().all()
        return jsonify({"customer_id": customer_id,
                        "addresses": [address.as_dict() for address in addresses]})

    @app.post("/customers/<int:customer_id>/addresses")
    def create_address(customer_id):
        data = validate(request.get_json(), profile=False)
        customer_record(customer_id, lock=True)
        existing_count = Address.query.filter_by(customer_id=customer_id).count()
        if existing_count >= MAX_ADDRESSES:
            raise Conflict("A customer can have at most 20 saved addresses.")
        make_default = data.pop("is_default", False) or existing_count == 0
        address = Address(customer_id=customer_id, is_default=False, **data)
        db.session.add(address)
        db.session.flush()
        if make_default:
            choose_default(customer_id, address)
        db.session.commit()
        response = jsonify({"address": address.as_dict()})
        response.status_code = 201
        response.headers["Location"] = f"/customers/{customer_id}/addresses/{address.id}"
        return response

    @app.get("/customers/<int:customer_id>/addresses/<int:address_id>")
    def get_address(customer_id, address_id):
        customer_record(customer_id)
        return jsonify({"address": address_record(customer_id, address_id).as_dict()})

    @app.patch("/customers/<int:customer_id>/addresses/<int:address_id>")
    def update_address(customer_id, address_id):
        data = validate(request.get_json(), profile=False, partial=True)
        customer_record(customer_id, lock=True)
        address = address_record(customer_id, address_id)
        if data.get("is_default") is False and address.is_default:
            raise Conflict("Choose another default address before clearing this default.")
        make_default = data.pop("is_default", None)
        for field, value in data.items():
            setattr(address, field, value)
        if make_default is True:
            choose_default(customer_id, address)
        db.session.commit()
        return jsonify({"address": address.as_dict()})

    @app.delete("/customers/<int:customer_id>/addresses/<int:address_id>")
    def delete_address(customer_id, address_id):
        customer_record(customer_id, lock=True)
        address = address_record(customer_id, address_id)
        if address.is_default:
            replacement = db.session.execute(select(Address).where(
                Address.customer_id == customer_id, Address.id != address.id
            ).order_by(Address.id)).scalars().first()
            if replacement is not None:
                replacement.is_default = True
        db.session.delete(address)
        db.session.commit()
        return "", 204

    install_observability(app,"customer")
    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8083, debug=False, use_reloader=False)
