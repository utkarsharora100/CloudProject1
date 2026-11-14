"""Validated restaurant data and compatibility with the original menu records."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
import unicodedata


class ValidationError(ValueError):
    pass


class MissingRecord(Exception):
    pass


class DuplicateRecord(Exception):
    pass


class StaleVersion(Exception):
    pass


class InvalidStoredData(Exception):
    pass


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value):
        raise ValidationError("Identifiers must contain 1 to 64 letters, digits, underscores or hyphens and start with a letter or digit.")
    return value


def text(value, field, limit, *, empty=False):
    if not isinstance(value, str):
        raise ValidationError(f"{field} must be text.")
    value = value.strip()
    if (not value and not empty) or len(value) > limit or any(unicodedata.category(c) in {"Cc", "Cs"} for c in value):
        raise ValidationError(f"{field} has invalid content or length.")
    return value


def boolean(value, field):
    if type(value) is not bool:
        raise ValidationError(f"{field} must be a boolean.")
    return value


def integer(value, field, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(f"{field} must be an integer between {minimum} and {maximum}.")
    return value


def profile(payload, *, partial=False):
    allowed = {"name", "description", "is_open", "estimated_prep_time_mins"}
    if not partial:
        allowed.add("currency")
    if not isinstance(payload, dict) or not payload or set(payload) - allowed:
        raise ValidationError("Provide a non-empty object containing supported restaurant fields.")
    if not partial and not {"name", "estimated_prep_time_mins"} <= set(payload):
        raise ValidationError("name and estimated_prep_time_mins are required.")
    clean = {}
    for key, value in payload.items():
        if key == "name":
            clean[key] = text(value, key, 120)
        elif key == "description":
            clean[key] = text(value, key, 500, empty=True)
        elif key == "is_open":
            clean[key] = boolean(value, key)
        elif key == "estimated_prep_time_mins":
            clean[key] = integer(value, key, 1, 240)
        elif value != "INR":
            raise ValidationError("Only INR is supported for this project.")
        else:
            clean[key] = "INR"
    if not partial:
        clean = {"description": "", "is_open": False, "currency": "INR", **clean}
    return clean


def menu_item(payload):
    allowed = {"name", "description", "price_minor", "available"}
    if not isinstance(payload, dict) or set(payload) - allowed or not {"name", "price_minor"} <= set(payload):
        raise ValidationError("Menu items require name and price_minor and only supported fields.")
    return {"name": text(payload["name"], "name", 120),
            "description": text(payload.get("description", ""), "description", 500, empty=True),
            "price_minor": integer(payload["price_minor"], "price_minor", 1, 100000000),
            "available": boolean(payload.get("available", True), "available")}


def legacy_price_minor(value):
    if isinstance(value, bool):
        raise InvalidStoredData("Stored menu price needs repair.")
    try:
        amount = Decimal(str(value)) * 100
        if not amount.is_finite() or amount != amount.to_integral_value():
            raise InvalidStoredData("Stored menu price needs repair.")
        return integer(int(amount), "price_minor", 1, 100000000)
    except (InvalidOperation, ValueError):
        raise InvalidStoredData("Stored menu price needs repair.") from None


def normalize(snapshot):
    raw = snapshot.to_dict()
    try:
        clean = profile({key: raw[key] for key in ("name", "description", "is_open", "estimated_prep_time_mins", "currency") if key in raw})
        version = integer(raw.get("version", 1), "version", 1, 9223372036854775806)
        items = raw.get("menu", [])
        if not isinstance(items, list) or len(items) > 100:
            raise ValidationError("Invalid stored menu.")
        normalized_items = []
        seen = set()
        for item in items:
            item_id = identifier(item["item_id"])
            if item_id in seen:
                raise ValidationError("Duplicate stored menu identifiers.")
            seen.add(item_id)
            price = item["price_minor"] if "price_minor" in item else legacy_price_minor(item["price"])
            clean_item = menu_item({"name": item["name"], "price_minor": price,
                                    "description": item.get("description", ""),
                                    "available": item.get("available", True)})
            normalized_items.append({"item_id": item_id, **clean_item})
        return {"restaurant_id": identifier(snapshot.id), **clean, "menu": normalized_items,
                "version": version, "created_at": raw.get("created_at", snapshot.create_time),
                "updated_at": raw.get("updated_at", snapshot.update_time)}
    except (ValidationError, KeyError, TypeError):
        raise InvalidStoredData("Stored restaurant data needs repair.") from None


def timestamp(value):
    if not isinstance(value, datetime):
        raise InvalidStoredData("Stored restaurant timestamp needs repair.")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
