from sqlalchemy import create_engine, text
import os


DATABASE_URL = (
    "postgresql+psycopg://"
    f"dataforge:{os.getenv('DATAFORGE_DB_PASSWORD')}@localhost:5432/nordmarket"
)

engine = create_engine(DATABASE_URL)

tables = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]


with engine.connect() as connection:

    for table in tables:

        result = connection.execute(
            text(f"SELECT COUNT(*) FROM {table}")
        )

        count = result.scalar()

        print(f"{table:15} {count:,} rows")