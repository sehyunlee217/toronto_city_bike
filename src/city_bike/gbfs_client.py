import httpx

STATION_INFORMATION_URL = (
    "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information"
)

STATION_STATUS_URL = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_status"


def fetch_json(url: str) -> dict:
    response = httpx.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def fetch_station_information() -> list[dict]:
    payload = fetch_json(STATION_INFORMATION_URL)
    return payload["data"]["stations"]


def fetch_station_status() -> list[dict]:
    payload = fetch_json(STATION_STATUS_URL)
    return payload["data"]["stations"]
