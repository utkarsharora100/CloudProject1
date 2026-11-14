"""Exercise Customer Service against the real local MySQL database."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import pymysql
from local.environment import require_local_emulator


BASE = "http://customer-service:8083"


def call(method, path, payload=None):
    request = Request(BASE + path, method=method,
                      data=None if payload is None else json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json"})
    try:
        response = urlopen(request, timeout=10)
    except HTTPError as error:
        response = error
    with response:
        raw = response.read()
        return response.status, json.loads(raw) if raw else None


def address_payload(label):
    return {"label": label, "line1": "Synthetic demo block", "city": "Demo City",
            "region": "Demo Region", "postal_code": "000000", "country_code": "in"}


def exercise_customer_service():
    require_local_emulator()
    code, health = call("GET", "/health")
    assert code == 200 and health["service"] == "customer-service"
    email = "demo-" + uuid.uuid4().hex + "@example.test"
    code, created = call("POST", "/customers", {"name": "Demo Customer", "email": email})
    assert code == 201, "Customer creation failed."
    customer_id = created["customer"]["id"]
    path = f"/customers/{customer_id}"
    code, changed = call("PATCH", path, {"name": "Updated Demo Customer"})
    assert code == 200 and changed["customer"]["name"] == "Updated Demo Customer"
    code, _ = call("POST", "/customers", {"name": "Duplicate Demo", "email": email.upper()})
    assert code == 409, "Duplicate normalized email was accepted."
    code, fetched = call("GET", path)
    assert code == 200 and fetched["customer"]["name"] == "Updated Demo Customer"
    print("Customer profile create/read/update and unique email: PASS", flush=True)

    code, _ = call("POST", path + "/addresses", {"city": "Incomplete Demo"})
    assert code == 400, "An incomplete address was accepted."
    code, first = call("POST", path + "/addresses", address_payload("Demo home"))
    assert code == 201 and first["address"]["is_default"] is True
    first_id = first["address"]["id"]
    code, second = call("POST", path + "/addresses", address_payload("Demo campus"))
    assert code == 201 and second["address"]["is_default"] is False
    second_id = second["address"]["id"]
    code, _ = call("PATCH", f"{path}/addresses/{second_id}", {"is_default": True})
    assert code == 200
    code, listed = call("GET", path + "/addresses")
    assert code == 200 and sum(a["is_default"] for a in listed["addresses"]) == 1
    assert next(a for a in listed["addresses"] if a["is_default"])["id"] == second_id

    def competing_default(index):
        return call("POST", path + "/addresses",
                    {**address_payload(f"Concurrent demo {index}"), "is_default": True})
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(competing_default, (1, 2)))
    assert all(code == 201 for code, _ in results), "Concurrent address creation failed."
    code, listed = call("GET", path + "/addresses")
    assert code == 200 and len(listed["addresses"]) == 4
    assert sum(a["is_default"] for a in listed["addresses"]) == 1, "Multiple defaults after concurrent requests."
    for _, result in results:
        code, _ = call("DELETE", f"{path}/addresses/{result['address']['id']}")
        assert code == 204
    code, listed = call("GET", path + "/addresses")
    assert code == 200 and len(listed["addresses"]) == 2
    assert sum(a["is_default"] for a in listed["addresses"]) == 1
    assert next(a for a in listed["addresses"] if a["is_default"])["id"] == first_id
    print("Saved addresses, concurrent default selection and deletion fallback: PASS", flush=True)

    with pymysql.connect(host=os.environ["DB_PRIVATE_IP"],
                         port=int(os.environ.get("DB_PORT", "3306")),
                         user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
                         database=os.environ["DB_NAME"], connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT name, email FROM customers WHERE id=%s", (customer_id,))
            assert cursor.fetchone() == ("Updated Demo Customer", email)
            cursor.execute("SELECT COUNT(*), SUM(is_default) FROM customer_addresses WHERE customer_id=%s", (customer_id,))
            assert tuple(map(int, cursor.fetchone())) == (2, 1)
    print("Customer and address records persisted in the real local MySQL database: PASS", flush=True)
    return {"customer_id": customer_id, "address_id": first_id,
            "second_address_id": second_id}


def main():
    result = exercise_customer_service()
    print(json.dumps({"result": "PASS", **result}), flush=True)


if __name__ == "__main__":
    main()
