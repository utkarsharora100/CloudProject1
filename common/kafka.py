"""Kafka settings with an explicit opt-in for isolated local development."""
import os


def kafka_config(client_id, environ=None):
    env = os.environ if environ is None else environ
    bootstrap = env.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be set.")
    protocol = env.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL").upper()
    config = {
        "bootstrap.servers": bootstrap,
        "security.protocol": protocol,
        "client.id": client_id,
    }
    if protocol == "PLAINTEXT":
        if env.get("APP_ENV", "production").lower() != "local":
            raise ValueError("PLAINTEXT Kafka requires APP_ENV=local.")
    elif protocol == "SASL_SSL":
        key = env.get("KAFKA_API_KEY")
        secret = env.get("KAFKA_API_SECRET")
        if not key or not secret:
            raise ValueError("SASL_SSL requires KAFKA_API_KEY and KAFKA_API_SECRET.")
        config.update({"sasl.mechanisms": "PLAIN", "sasl.username": key, "sasl.password": secret})
    else:
        raise ValueError("KAFKA_SECURITY_PROTOCOL must be SASL_SSL or PLAINTEXT.")
    return config
