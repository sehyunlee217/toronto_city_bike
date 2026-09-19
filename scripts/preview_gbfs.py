from city_bike.gbfs_client import fetch_station_information, fetch_station_status


def print_sample(title: str, rows: list[dict]) -> None:
    print(f"\n{title}")
    print("=" * len(title))
    print(f"count: {len(rows)}")

    if not rows:
        return

    sample = rows[0]
    for key, value in sample.items():
        print(f"{key}: {value}")


def main() -> None:
    stations = fetch_station_information()
    statuses = fetch_station_status()

    print_sample("Station information sample", stations)
    print_sample("Station status sample", statuses)


if __name__ == "__main__":
    main()
