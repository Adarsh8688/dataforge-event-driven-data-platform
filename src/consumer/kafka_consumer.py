import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

topics = [
    topic.strip()
    for topic in os.getenv("KAFKA_TOPICS", "").split(",")
    if topic.strip()
]

bronze_root = PROJECT_ROOT / os.getenv("BRONZE_ROOT", "data/bronze")

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    group_id=os.getenv("KAFKA_GROUP_ID", "dataforge-bronze-consumer-v1"),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    request_timeout_ms=60000,
    session_timeout_ms=30000,
    max_poll_interval_ms=900000,
    max_poll_records=5000,
    fetch_max_bytes=52428800,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Listening to {len(topics)} topics...")
print("Batch mode enabled.")

try:
    while True:
        records = consumer.poll(
            timeout_ms=1000,
            max_records=5000,
        )

        if not records:
            print("Kafka backlog consumed.")
            break

        files = {}
        message_count = 0

        for topic_partition, messages in records.items():
            for message in messages:
                table_name = message.topic.split(".")[-1]
                date_part = datetime.now(timezone.utc).strftime("%Y-%m-%d")

                output_dir = (
                    bronze_root
                    / table_name
                    / f"ingestion_date={date_part}"
                )
                output_dir.mkdir(parents=True, exist_ok=True)

                output_file = output_dir / "events.jsonl"

                if output_file not in files:
                    files[output_file] = []

                event = {
                    "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
                    "kafka_topic": message.topic,
                    "kafka_partition": message.partition,
                    "kafka_offset": message.offset,
                    "kafka_key": (
                        message.key.decode("utf-8")
                        if message.key
                        else None
                    ),
                    "payload": message.value,
                }

                files[output_file].append(
                    json.dumps(event, separators=(",", ":"))
                )

                message_count += 1

        for output_file, lines in files.items():
            with output_file.open("a", encoding="utf-8") as file:
                file.write("\n".join(lines) + "\n")

        consumer.commit()

        print(f"Saved batch: {message_count} messages")

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
