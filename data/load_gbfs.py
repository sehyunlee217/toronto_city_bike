import os
import httpx
import psycopg
from psycopg.types.json import Jsonb

STATION_INFORMATION_URL = (
    "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information"
)
STATION_STATUS_URL = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_status"
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5555/city_bike",
)


def fetch_json(url):
    response = httpx.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def fetch_station_information():
    return fetch_json(STATION_INFORMATION_URL)["data"]["stations"]


def fetch_station_status():
    return fetch_json(STATION_STATUS_URL)["data"]["stations"]


def create_stations_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stations (
                station_id TEXT PRIMARY KEY,
                external_id TEXT,
                name TEXT NOT NULL,
                physical_configuration TEXT,
                lat DOUBLE PRECISION NOT NULL,
                lon DOUBLE PRECISION NOT NULL,
                altitude DOUBLE PRECISION,
                address TEXT,
                capacity INTEGER,
                is_charging_station BOOLEAN,
                rental_methods TEXT[],
                groups_array TEXT[],
                obcn TEXT,
                short_name TEXT,
                nearby_distance DOUBLE PRECISION,
                ride_code_support BOOLEAN,
                updated_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
    conn.commit()


def create_station_statuses_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS station_statuses (
                station_id TEXT PRIMARY KEY,
                num_bikes_available INTEGER,
                num_bikes_disabled INTEGER,
                is_charging_station BOOLEAN,
                status TEXT,
                num_bikes_available_types JSONB,
                num_docks_available INTEGER,
                num_docks_disabled INTEGER,
                last_reported BIGINT,
                is_installed BOOLEAN,
                is_renting BOOLEAN,
                is_returning BOOLEAN,
                updated_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
    conn.commit()


def normalize_station(station):
    return {
        "station_id": station["station_id"],
        "external_id": station.get("external_id"),
        "name": station["name"],
        "physical_configuration": station.get("physical_configuration"),
        "lat": station["lat"],
        "lon": station["lon"],
        "altitude": station.get("altitude"),
        "address": station.get("address"),
        "capacity": station.get("capacity"),
        "is_charging_station": station.get("is_charging_station"),
        "rental_methods": station.get("rental_methods", []),
        "groups": station.get("groups", []),
        "obcn": station.get("obcn"),
        "short_name": station.get("short_name"),
        "nearby_distance": station.get("nearby_distance"),
        "_ride_code_support": station.get("_ride_code_support"),
    }


def normalize_gbfs_bool(value):
    if value is None:
        return None
    return bool(value)


def normalize_station_status(status):
    return {
        "station_id": status["station_id"],
        "num_bikes_available": status.get("num_bikes_available"),
        "num_bikes_disabled": status.get("num_bikes_disabled"),
        "is_charging_station": status.get("is_charging_station"),
        "status": status.get("status"),
        "num_bikes_available_types": status.get("num_bikes_available_types", {}),
        "num_docks_available": status.get("num_docks_available"),
        "num_docks_disabled": status.get("num_docks_disabled"),
        "last_reported": status.get("last_reported"),
        "is_installed": normalize_gbfs_bool(status.get("is_installed")),
        "is_renting": normalize_gbfs_bool(status.get("is_renting")),
        "is_returning": normalize_gbfs_bool(status.get("is_returning")),
    }


def upsert_station(conn, station):
    station = normalize_station(station)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stations (
                station_id,
                external_id,
                name,
                physical_configuration,
                lat,
                lon,
                altitude,
                address,
                capacity,
                is_charging_station,
                rental_methods,
                groups_array,
                obcn,
                short_name,
                nearby_distance,
                ride_code_support
            )
            VALUES (
                %(station_id)s,
                %(external_id)s,
                %(name)s,
                %(physical_configuration)s,
                %(lat)s,
                %(lon)s,
                %(altitude)s,
                %(address)s,
                %(capacity)s,
                %(is_charging_station)s,
                %(rental_methods)s,
                %(groups)s,
                %(obcn)s,
                %(short_name)s,
                %(nearby_distance)s,
                %(_ride_code_support)s
            )
            ON CONFLICT (station_id)
            DO UPDATE SET
                external_id = EXCLUDED.external_id,
                name = EXCLUDED.name,
                physical_configuration = EXCLUDED.physical_configuration,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                altitude = EXCLUDED.altitude,
                address = EXCLUDED.address,
                capacity = EXCLUDED.capacity,
                is_charging_station = EXCLUDED.is_charging_station,
                rental_methods = EXCLUDED.rental_methods,
                groups_array = EXCLUDED.groups_array,
                obcn = EXCLUDED.obcn,
                short_name = EXCLUDED.short_name,
                nearby_distance = EXCLUDED.nearby_distance,
                ride_code_support = EXCLUDED.ride_code_support,
                updated_at = now();
            """,
            station,
        )


def upsert_station_status(conn, status):
    status = normalize_station_status(status)
    status["num_bikes_available_types"] = Jsonb(status["num_bikes_available_types"])

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO station_statuses (
                station_id,
                num_bikes_available,
                num_bikes_disabled,
                is_charging_station,
                status,
                num_bikes_available_types,
                num_docks_available,
                num_docks_disabled,
                last_reported,
                is_installed,
                is_renting,
                is_returning
            )
            VALUES (
                %(station_id)s,
                %(num_bikes_available)s,
                %(num_bikes_disabled)s,
                %(is_charging_station)s,
                %(status)s,
                %(num_bikes_available_types)s,
                %(num_docks_available)s,
                %(num_docks_disabled)s,
                %(last_reported)s,
                %(is_installed)s,
                %(is_renting)s,
                %(is_returning)s
            )
            ON CONFLICT (station_id)
            DO UPDATE SET
                num_bikes_available = EXCLUDED.num_bikes_available,
                num_bikes_disabled = EXCLUDED.num_bikes_disabled,
                is_charging_station = EXCLUDED.is_charging_station,
                status = EXCLUDED.status,
                num_bikes_available_types = EXCLUDED.num_bikes_available_types,
                num_docks_available = EXCLUDED.num_docks_available,
                num_docks_disabled = EXCLUDED.num_docks_disabled,
                last_reported = EXCLUDED.last_reported,
                is_installed = EXCLUDED.is_installed,
                is_renting = EXCLUDED.is_renting,
                is_returning = EXCLUDED.is_returning,
                updated_at = now();
            """,
            status,
        )


def load_stations():
    """
    Load station information.
    ======================================
    station_id | 7013
    name       | Scott St / The Esplanade
    address    | Scott St / The Esplanade
    capacity   | 19
    lat        | 43.64659663170443
    lon        | -79.37530913867988
    """
    stations = fetch_station_information()

    with psycopg.connect(DATABASE_URL) as conn:
        create_stations_table(conn)
        for station in stations:
            upsert_station(conn, station)
        conn.commit()

    print(f"Loaded {len(stations)} stations into Postgres @ {DATABASE_URL}")


def load_station_status():
    """
    Load station status.
    ======================================
    station_id                : 7013
    num_bikes_available       : 22
    num_bikes_disabled        : 2
    is_charging_station       : false
    status                    : "IN_SERVICE"
    num_bikes_available_types : { mechanical: 21, ebike: 1 }
    num_docks_available       : 23
    num_docks_disabled        : 0
    last_reported             : 1779936900
    is_installed              : 1
    is_renting                : 1
    is_returning              : 1
    """

    statuses = fetch_station_status()

    with psycopg.connect(DATABASE_URL) as conn:
        create_station_statuses_table(conn)
        for status in statuses:
            upsert_station_status(conn, status)
        conn.commit()

    print(f"Loaded {len(statuses)} station statuses into Postgres @ {DATABASE_URL}")


if __name__ == "__main__":
    load_stations()
    load_station_status()
