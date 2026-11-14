"""Restaurant gRPC API with transactional Firestore-backed management."""
from concurrent import futures
from functools import wraps
import logging
import grpc
from google.api_core.exceptions import GoogleAPICallError
from restaurant_service import restaurant_pb2 as pb, restaurant_pb2_grpc as rpc
from restaurant_service.domain import (DuplicateRecord, InvalidStoredData, MissingRecord,
                                      StaleVersion, ValidationError, timestamp)
from restaurant_service.repository import local_repository


logger = logging.getLogger(__name__)


def guarded(method):
    @wraps(method)
    def run(self, request, context):
        try:
            return method(self, request, context)
        except ValidationError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        except MissingRecord as error:
            context.abort(grpc.StatusCode.NOT_FOUND, str(error))
        except DuplicateRecord as error:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, str(error))
        except StaleVersion as error:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
        except InvalidStoredData:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Stored restaurant data needs repair.")
        except GoogleAPICallError:
            logger.error("Restaurant storage operation failed.")
            context.abort(grpc.StatusCode.UNAVAILABLE, "Restaurant data is temporarily unavailable.")
        except Exception:
            logger.error("Unexpected restaurant operation failure.")
            context.abort(grpc.StatusCode.INTERNAL, "Restaurant operation could not be completed.")
    return run


def restaurant_message(data):
    return pb.Restaurant(**{key: data[key] for key in (
        "restaurant_id", "name", "is_open", "estimated_prep_time_mins", "currency", "description", "version")},
        created_at=timestamp(data["created_at"]), updated_at=timestamp(data["updated_at"]))


def item_message(item):
    return pb.MenuItem(item_id=item["item_id"], name=item["name"],
                       price=item["price_minor"] / 100, price_minor=item["price_minor"],
                       available=item["available"], description=item["description"])


class RestaurantServiceServicer(rpc.RestaurantServiceServicer):
    def __init__(self, repository):
        self.repository = repository

    @guarded
    def GetMenu(self, request, context):
        data = self.repository.get(request.restaurant_id)
        return pb.MenuResponse(restaurant_id=data["restaurant_id"], is_open=data["is_open"],
                               items=[item_message(item) for item in data["menu"]],
                               estimated_prep_time_mins=data["estimated_prep_time_mins"],
                               name=data["name"], currency=data["currency"], version=data["version"])

    @guarded
    def UpdateStatus(self, request, context):
        data = self.repository.update(request.restaurant_id, {"is_open": request.is_open}, request.expected_version)
        return pb.StatusResponse(success=True, version=data["version"])

    @guarded
    def ListRestaurants(self, request, context):
        page, token = self.repository.list(request.page_size or 20, request.page_token)
        return pb.RestaurantList(restaurants=[restaurant_message(data) for data in page], next_page_token=token)

    @guarded
    def CreateRestaurant(self, request, context):
        payload = {key: getattr(request, key) for key in (
            "name", "description", "is_open", "estimated_prep_time_mins")}
        payload["currency"] = request.currency or "INR"
        return restaurant_message(self.repository.create(request.restaurant_id, payload))

    @guarded
    def GetRestaurant(self, request, context):
        return restaurant_message(self.repository.get(request.restaurant_id))

    @guarded
    def UpdateRestaurant(self, request, context):
        fields = list(request.update_fields)
        allowed = {"name", "description", "is_open", "estimated_prep_time_mins"}
        if not fields or len(fields) != len(set(fields)) or not set(fields) <= allowed:
            raise ValidationError("Specify unique supported restaurant update fields.")
        payload = {key: getattr(request.restaurant, key) for key in fields}
        return restaurant_message(self.repository.update(request.restaurant.restaurant_id, payload, request.expected_version))

    @guarded
    def UpsertMenuItem(self, request, context):
        if not request.item.HasField("price_minor"):
            raise ValidationError("price_minor is required for menu writes.")
        payload = {"name": request.item.name, "description": request.item.description,
                   "price_minor": request.item.price_minor,
                   "available": request.item.available if request.item.HasField("available") else True}
        data, item, created = self.repository.put_item(request.restaurant_id, request.item.item_id,
                                                       payload, request.expected_version)
        return pb.MenuItemResult(restaurant_id=data["restaurant_id"], item=item_message(item),
                                 created=created, version=data["version"])

    @guarded
    def RemoveMenuItem(self, request, context):
        data = self.repository.remove_item(request.restaurant_id, request.item_id, request.expected_version)
        return pb.StatusResponse(success=True, version=data["version"])


def build_server(repository, address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rpc.add_RestaurantServiceServicer_to_server(RestaurantServiceServicer(repository), server)
    port = server.add_insecure_port(address)
    if not port:
        raise RuntimeError("Restaurant gRPC port could not be bound.")
    return server, port


def serve():
    server, _ = build_server(local_repository(), "0.0.0.0:50051")
    server.start()
    print("Local Restaurant gRPC Service started on port 50051", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(5).wait()


if __name__ == "__main__":
    serve()
