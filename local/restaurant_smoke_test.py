"""Real HTTP -> gRPC -> Firestore emulator verification using fictional records."""
from concurrent.futures import ThreadPoolExecutor
import json
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import grpc
from google.cloud import firestore
from local.environment import require_local_emulator
from restaurant_service import restaurant_pb2 as pb, restaurant_pb2_grpc as rpc


BASE = "http://restaurant-api:8084"


def call(method, path, payload=None, headers=None):
    request = Request(BASE + path, method=method,
                      data=None if payload is None else json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json", **(headers or {})})
    try:
        response = urlopen(request, timeout=20)
    except HTTPError as error:
        response = error
    with response:
        content = response.read()
        return response.status, json.loads(content) if content else None, response.headers


def exercise_restaurant_service():
    project = require_local_emulator()
    code, healthy, _ = call("GET", "/health")
    assert code == 200 and healthy["grpc_connected"]
    code, created, headers = call("POST", "/restaurants", {
        "name": "Demo Restaurant", "estimated_prep_time_mins": 12})
    assert code == 201 and created["restaurant"]["is_open"] is False
    restaurant_id = created["restaurant"]["restaurant_id"]
    path = f"/restaurants/{restaurant_id}"
    assert headers["Location"] == path and headers["ETag"] == '"1"'
    code, empty, _ = call("GET", path + "/menu")
    assert code == 200 and empty["items"] == []
    for item_id, price in (("veg-meal", 12999), ("paneer-wrap", 9950)):
        code, item, _ = call("PUT", path + "/menu/" + item_id,
                             {"name": "Demo " + item_id, "price_minor": price})
        assert code == 201 and item["item"]["price_minor"] == price
    code, _, _ = call("PUT", path + "/menu/bad-price", {"name": "Invalid demo", "price_minor": 12.99})
    assert code == 400
    code, before, headers = call("GET", path)
    assert code == 200
    stale_etag = headers["ETag"]
    code, changed, _ = call("PATCH", path, {"name": "Updated Demo Restaurant", "is_open": True,
                                          "estimated_prep_time_mins": 18}, {"If-Match": stale_etag})
    assert code == 200 and changed["restaurant"]["estimated_prep_time_mins"] == 18
    code, _, _ = call("PATCH", path, {"is_open": False}, {"If-Match": stale_etag})
    assert code == 412, "Stale restaurant edit was accepted."
    print("Restaurant HTTP/gRPC create/read/edit, precise prices and stale-edit protection: PASS", flush=True)

    def add_concurrently(index):
        return call("PUT", f"{path}/menu/concurrent-{index}",
                    {"name": f"Concurrent demo {index}", "price_minor": 5000 + index})
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(add_concurrently, (1, 2)))
    assert all(code == 201 for code, _, _ in results), "Concurrent menu writes failed."
    code, menu, _ = call("GET", path + "/menu")
    assert code == 200 and len(menu["items"]) == 4, "A concurrent menu update was lost."
    assert menu["is_open"] and menu["estimated_prep_time_mins"] == 18
    for index in (1, 2):
        code, _, _ = call("DELETE", f"{path}/menu/concurrent-{index}")
        assert code == 204
    code, _, _ = call("PUT", path + "/menu/paneer-wrap", {
        "name": "Demo paneer-wrap", "price_minor": 9950, "available": False})
    assert code == 200
    print("Concurrent Firestore menu writes preserve both items; deletion and availability: PASS", flush=True)

    with grpc.insecure_channel("restaurant-service:50051") as channel:
        service = rpc.RestaurantServiceStub(channel)
        service.UpdateStatus(pb.StatusRequest(restaurant_id=restaurant_id, is_open=False), timeout=10)
        menu = service.GetMenu(pb.RestaurantRequest(restaurant_id=restaurant_id), timeout=10)
        assert not menu.is_open and len(menu.items) == 2 and menu.estimated_prep_time_mins == 18
        item = next(item for item in menu.items if item.item_id == "veg-meal")
        assert item.price_minor == 12999 and abs(item.price - 129.99) < 0.001
        assert next(item for item in menu.items if item.item_id == "paneer-wrap").available is False
        service.UpdateStatus(pb.StatusRequest(restaurant_id=restaurant_id, is_open=True), timeout=10)
        missing_id = "missing-" + uuid.uuid4().hex
        try:
            service.UpdateStatus(pb.StatusRequest(restaurant_id=missing_id, is_open=True), timeout=10)
            raise AssertionError("Opening status created a nonexistent restaurant.")
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.NOT_FOUND
        try:
            service.UpdateRestaurant(pb.UpdateRestaurantRequest(
                restaurant=pb.Restaurant(restaurant_id=restaurant_id, estimated_prep_time_mins=0),
                update_fields=["estimated_prep_time_mins"]), timeout=10)
            raise AssertionError("Invalid gRPC preparation estimate was accepted.")
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
    print("Original gRPC methods/price field and shared validation remain compatible: PASS", flush=True)

    # Exercise document-ID cursor paging against the actual emulator without
    # changing any pre-existing sample restaurant.
    found = False
    token = ""
    previous = set()
    for _ in range(100):
        code, page, _ = call("GET", "/restaurants?limit=2" + ("&after=" + token if token else ""))
        assert code == 200
        ids = [item["restaurant_id"] for item in page["restaurants"]]
        assert ids == sorted(ids) and not previous.intersection(ids)
        previous.update(ids)
        found = found or restaurant_id in ids
        token = page["next_page_token"]
        if token is None:
            break
    assert token is None and found, "Restaurant pagination failed."
    database = firestore.Client(project=project)
    raw = database.collection("restaurants").document(restaurant_id).get(retry=None, timeout=5).to_dict()
    assert raw["is_open"] and raw["estimated_prep_time_mins"] == 18
    assert len(raw["menu"]) == 2 and all(type(item["price_minor"]) is int for item in raw["menu"])
    assert not database.collection("restaurants").document(missing_id).get(retry=None, timeout=5).exists
    code, final, _ = call("GET", path)
    assert code == 200 and final["restaurant"]["version"] == raw["version"]
    print("Restaurant listing/pagination and actual Firestore persistence: PASS", flush=True)
    return {"restaurant_id": restaurant_id, "version": raw["version"]}


def main():
    print(json.dumps({"result": "PASS", **exercise_restaurant_service()}), flush=True)


if __name__ == "__main__":
    main()
