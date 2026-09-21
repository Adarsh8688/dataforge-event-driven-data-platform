import socket
import sys
import os

import psycopg2


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nordmarket",
    "user": "dataforge",
    "password":os.getenv("DATAFORGE_DB_PASSWORD")
}


def check_postgres():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print("PostgreSQL: UP")
        return True
    except Exception as exc:
        print(f"PostgreSQL: DOWN - {exc}")
        return False


def check_tcp_service(name, host, port):
    try:
        with socket.create_connection((host, port), timeout=3):
            print(f"{name}: UP")
            return True
    except OSError as exc:
        print(f"{name}: DOWN - {exc}")
        return False


def main():
    checks = [
        check_postgres(),
        check_tcp_service("Kafka", "localhost", 9092),
        check_tcp_service("Schema Registry", "localhost", 8081),
    ]

    print()

    if all(checks):
        print("MONITORING HEALTH CHECK PASSED")
        return

    print("MONITORING HEALTH CHECK FAILED")
    sys.exit(1)


if __name__ == "__main__":
    main()