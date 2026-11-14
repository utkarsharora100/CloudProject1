"""Create only the additive v2 order tables in the isolated local database."""
from sqlalchemy import text
from order_service.models import db
from order_service.storage import create_storage_app


def main():
    app = create_storage_app({"AUTO_CREATE_SCHEMA": True})
    with app.app_context():
        if db.engine.dialect.name == "mysql":
            with db.engine.begin() as connection:
                collation = connection.execute(text(
                    "SELECT COLLATION_NAME FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='order_requests_v2' AND COLUMN_NAME='key'"
                )).scalar_one()
                if collation != "ascii_bin":
                    # Values already satisfy the API's ASCII contract. This
                    # preserves their bytes and changes only key comparison.
                    connection.execute(text(
                        "ALTER TABLE order_requests_v2 MODIFY COLUMN `key` "
                        "VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL"
                    ))
    print("Local v2 order schema is ready with case-sensitive keys; legacy tables are preserved.", flush=True)


if __name__ == "__main__":
    main()
