"""Preview the separated streams without sending records to Kafka."""
import json
from city_bike.observations import collect_observations


def main():
    batch = collect_observations(8)
    for name, rows in batch.items():
        size = sum(len(json.dumps(row, separators=(",", ":")).encode()) for row in rows)
        print(f"{name}: {len(rows)} records, {size:,} uncompressed JSON bytes")
        print(json.dumps(rows[0] if rows else None, indent=2))


if __name__ == "__main__":
    main()
