from common.observability import install as install_observability
from common.deployment import require_profile,service_endpoint
"""Local browser-friendly HTTP adapter; all domain operations go through gRPC."""
import atexit
import os
import re
import uuid
import grpc
from flask import Flask, jsonify, request
from werkzeug.exceptions import BadRequest, HTTPException
from restaurant_service import restaurant_pb2 as pb, restaurant_pb2_grpc as rpc
from restaurant_service.domain import ValidationError, identifier, menu_item, profile


TIMEOUT = 10
RPC_ERRORS = {
    grpc.StatusCode.INVALID_ARGUMENT: (400, "Restaurant request is invalid."),
    grpc.StatusCode.NOT_FOUND: (404, "Restaurant or menu item not found."),
    grpc.StatusCode.ALREADY_EXISTS: (409, "Restaurant already exists."),
    grpc.StatusCode.FAILED_PRECONDITION: (412, "Restaurant version changed or stored data needs repair; reload before retrying."),
    grpc.StatusCode.UNAVAILABLE: (503, "Restaurant data is temporarily unavailable."),
    grpc.StatusCode.DEADLINE_EXCEEDED: (503, "Restaurant request timed out; check its current state before retrying."),
}


def restaurant_dict(value):
    return {key: getattr(value, key) for key in ("restaurant_id", "name", "description", "is_open",
            "estimated_prep_time_mins", "currency", "version", "created_at", "updated_at")}


def item_dict(value):
    return {key: getattr(value, key) for key in ("item_id", "name", "description", "price_minor", "available")}


def create_app(config=None, *, stub=None):
    app = Flask(__name__)
    app.config.update(APP_ENV=os.environ.get("APP_ENV", "production"), MAX_CONTENT_LENGTH=32 * 1024)
    if config:
        app.config.update(config)
    if not app.testing:require_profile(app.config["APP_ENV"])
    if stub is None:
        target = os.environ.get("RESTAURANT_GRPC_TARGET", "127.0.0.1:50051")
        service_endpoint(target,{"restaurant-service:50051","localhost:50051","127.0.0.1:50051"},grpc=True)
        channel = grpc.insecure_channel(target)
        atexit.register(channel.close)
        stub = rpc.RestaurantServiceStub(channel)

    @app.after_request
    def response_headers(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.errorhandler(ValidationError)
    def validation_error(error):
        return jsonify({"error": str(error)}), 400

    @app.errorhandler(HTTPException)
    def http_error(error):
        response = error.get_response()
        response.data = app.json.dumps({"error": error.description})
        response.content_type = "application/json"
        return response

    @app.errorhandler(grpc.RpcError)
    def rpc_error(error):
        status, message = RPC_ERRORS.get(error.code(), (500, "Restaurant operation could not be completed."))
        # Never forward untrusted upstream error.details(), credentials or traces.
        return jsonify({"error": message}), status

    @app.errorhandler(Exception)
    def unexpected_error(error):
        app.logger.error("Unexpected restaurant HTTP operation failure.")
        return jsonify({"error": "Restaurant operation could not be completed."}), 500

    def expected_version():
        value = request.headers.get("If-Match")
        if value is None:
            return 0
        match = re.fullmatch(r'"([1-9][0-9]{0,17})"', value)
        if match is None:
            raise BadRequest('If-Match must contain one quoted positive version number.')
        return int(match.group(1))

    def versioned(payload, version, status=200):
        response = jsonify(payload)
        response.status_code = status
        response.set_etag(str(version))
        return response

    @app.get("/health")
    def health():
        stub.ListRestaurants(pb.ListRequest(page_size=1), timeout=TIMEOUT)
        return jsonify({"status": "healthy", "service": "restaurant-api", "grpc_connected": True})

    @app.get("/restaurants")
    def list_restaurants():
        if set(request.args) - {"limit", "after"}:
            raise BadRequest("Unsupported restaurant-list query parameters.")
        limit = request.args.get("limit", "20")
        if not re.fullmatch(r"[0-9]{1,2}", limit) or not 1 <= int(limit) <= 50:
            raise BadRequest("limit must be an integer between 1 and 50.")
        token = request.args.get("after", "")
        if token:
            identifier(token)
        result = stub.ListRestaurants(pb.ListRequest(page_size=int(limit), page_token=token), timeout=TIMEOUT)
        return jsonify({"restaurants": [restaurant_dict(item) for item in result.restaurants],
                        "next_page_token": result.next_page_token or None})

    @app.post("/restaurants")
    def create_restaurant():
        data = profile(request.get_json())
        restaurant_id = uuid.uuid4().hex
        result = stub.CreateRestaurant(pb.Restaurant(restaurant_id=restaurant_id, **data), timeout=TIMEOUT)
        response = versioned({"restaurant": restaurant_dict(result)}, result.version, 201)
        response.headers["Location"] = f"/restaurants/{result.restaurant_id}"
        return response

    @app.get("/restaurants/<restaurant_id>")
    def get_restaurant(restaurant_id):
        result = stub.GetRestaurant(pb.RestaurantRequest(restaurant_id=identifier(restaurant_id)), timeout=TIMEOUT)
        return versioned({"restaurant": restaurant_dict(result)}, result.version)

    @app.patch("/restaurants/<restaurant_id>")
    def update_restaurant(restaurant_id):
        data = profile(request.get_json(), partial=True)
        result = stub.UpdateRestaurant(pb.UpdateRestaurantRequest(
            restaurant=pb.Restaurant(restaurant_id=identifier(restaurant_id), **data),
            update_fields=list(data), expected_version=expected_version()), timeout=TIMEOUT)
        return versioned({"restaurant": restaurant_dict(result)}, result.version)

    @app.get("/restaurants/<restaurant_id>/menu")
    def get_menu(restaurant_id):
        result = stub.GetMenu(pb.RestaurantRequest(restaurant_id=identifier(restaurant_id)), timeout=TIMEOUT)
        return versioned({"restaurant_id": result.restaurant_id, "name": result.name,
                          "is_open": result.is_open, "estimated_prep_time_mins": result.estimated_prep_time_mins,
                          "currency": result.currency, "version": result.version,
                          "items": [item_dict(item) for item in result.items]}, result.version)

    @app.put("/restaurants/<restaurant_id>/menu/<item_id>")
    def put_item(restaurant_id, item_id):
        data = menu_item(request.get_json())
        result = stub.UpsertMenuItem(pb.MenuItemRequest(
            restaurant_id=identifier(restaurant_id), item=pb.MenuItem(item_id=identifier(item_id), **data),
            expected_version=expected_version()), timeout=TIMEOUT)
        return versioned({"restaurant_id": result.restaurant_id, "item": item_dict(result.item),
                          "version": result.version}, result.version, 201 if result.created else 200)

    @app.delete("/restaurants/<restaurant_id>/menu/<item_id>")
    def remove_item(restaurant_id, item_id):
        result = stub.RemoveMenuItem(pb.RemoveMenuItemRequest(
            restaurant_id=identifier(restaurant_id), item_id=identifier(item_id),
            expected_version=expected_version()), timeout=TIMEOUT)
        response = app.response_class(status=204)
        response.set_etag(str(result.version))
        return response

    install_observability(app,"restaurant")
    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8084, debug=False, use_reloader=False)
