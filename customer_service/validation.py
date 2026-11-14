"""Validate known fields without echoing submitted personal data in errors."""
import re
import unicodedata
from werkzeug.exceptions import BadRequest


PROFILE_FIELDS = {"name": 100, "email": 254, "phone": 32}
ADDRESS_FIELDS = {"label": 40, "line1": 200, "line2": 200, "city": 80,
                  "region": 80, "postal_code": 20, "country_code": 2, "is_default": None}


def validate(payload, *, profile, partial=False):
    if not isinstance(payload, dict) or not payload:
        raise BadRequest("A non-empty JSON object is required.")
    fields = PROFILE_FIELDS if profile else ADDRESS_FIELDS
    required = {"name", "email"} if profile else {"label", "line1", "city", "postal_code", "country_code"}
    optional = {"phone"} if profile else {"line2", "region"}
    if set(payload) - set(fields):
        raise BadRequest("The request contains unsupported fields.")
    if not partial and required - set(payload):
        raise BadRequest("Required profile fields are missing." if profile else "Required address fields are missing.")
    clean = {}
    for field, value in payload.items():
        if field == "is_default":
            if type(value) is not bool:
                raise BadRequest("is_default must be a boolean.")
            clean[field] = value
            continue
        if field in optional and value is None:
            clean[field] = None
            continue
        if not isinstance(value, str):
            raise BadRequest(f"{field} must be text.")
        value = value.strip()
        if not value and field in optional:
            clean[field] = None
            continue
        if not value or any(unicodedata.category(char) in {"Cc", "Cs"} for char in value):
            raise BadRequest(f"{field} must contain valid, non-empty text.")
        if field == "email":
            value = value.casefold()
            if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
                raise BadRequest("email must be a valid email address.")
        elif field == "phone":
            if not re.fullmatch(r"[+0-9(). -]+", value) or not 7 <= sum(c.isdigit() for c in value) <= 15:
                raise BadRequest("phone must contain 7 to 15 digits with optional phone punctuation.")
        elif field == "country_code":
            if not re.fullmatch(r"[A-Za-z]{2}", value):
                raise BadRequest("country_code must contain two ASCII letters.")
            value = value.upper()
        if len(value) > fields[field]:
            raise BadRequest(f"{field} exceeds its maximum length.")
        clean[field] = value
    return clean
