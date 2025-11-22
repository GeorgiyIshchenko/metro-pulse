import argparse
import os
import shutil
import uuid
from datetime import datetime, timedelta, date, UTC
from pathlib import Path
import random

import numpy as np
import pandas as pd
from tqdm.auto import tqdm
import minio


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate synthetic MetroPulse data as Parquet files.")
    parser.add_argument("--output-dir", type=str, default="data",
                        help="Output directory for generated Parquet files.")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date (YYYY-MM-DD). Defaults to 7 days ago.")
    parser.add_argument("--days", type=int, default=7,
                        help="Number of days to generate data for.")
    parser.add_argument("--num-users", type=int, default=2000,
                        help="Number of users to generate.")
    parser.add_argument("--num-routes", type=int, default=40,
                        help="Number of routes to generate.")
    parser.add_argument("--num-stops", type=int, default=300,
                        help="Number of stops to generate.")
    parser.add_argument("--num-vehicles", type=int, default=120,
                        help="Number of vehicles to generate.")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed.")
    return parser.parse_args()


def random_dates(start_ts: datetime, end_ts: datetime, n: int) -> np.ndarray:
    """Generate n random timestamps between start_ts and end_ts."""
    delta = (end_ts - start_ts).total_seconds()
    rand_secs = np.random.rand(n) * delta
    return np.array([start_ts + timedelta(seconds=float(s)) for s in rand_secs])


# ============ FAIR PRODUCTS ============

def generate_fare_products() -> pd.DataFrame:
    rows = [
        ("FARE_SINGLE", "Single ride", "single",
         "EUR", 2.50, "Single ride in one zone"),
        ("FARE_DAILY", "Day pass", "daily", "EUR",
         6.00, "Unlimited rides during one day"),
        ("FARE_WEEKLY", "Weekly pass", "weekly", "EUR",
         20.00, "Unlimited rides during one week"),
    ]
    now = datetime.now(UTC)
    data = []
    for fare_product_id, name, ftype, currency, price, desc in rows:
        data.append(
            dict(
                fare_product_id=fare_product_id,
                product_name=name,
                fare_type=ftype,
                currency_code=currency,
                base_price=round(price, 2),
                description=desc,
                src_system="oltp",
                src_created_at=now - timedelta(days=30),
                src_updated_at=now - timedelta(days=1),
            )
        )
    return pd.DataFrame(data)


# ============ USERS ============

def generate_users(num_users: int, start_date: date, days: int) -> pd.DataFrame:
    user_ids = [f"USER-{i:06d}" for i in range(1, num_users + 1)]
    base_ts_start = datetime.combine(
        start_date - timedelta(days=30), datetime.min.time())
    base_ts_end = datetime.combine(start_date, datetime.min.time())

    registration_ts = random_dates(base_ts_start, base_ts_end, num_users)
    statuses = np.random.choice(["active", "blocked", "deleted"], size=num_users,
                                p=[0.9, 0.05, 0.05])

    data = []
    for i, user_id in enumerate(user_ids):
        uid = uuid.uuid4()
        email = f"user{i+1}@example.com"
        phone = f"+7-900-{random.randint(1000000, 9999999):07d}"
        full_name = f"User {i+1}"
        reg_ts = registration_ts[i]
        status = statuses[i]
        src_created_at = reg_ts
        src_updated_at = reg_ts + timedelta(days=random.randint(0, 10))

        data.append(
            dict(
                user_id=user_id,
                user_uuid=str(uid),  # важный фикс для pyarrow
                email=email,
                phone=phone,
                full_name=full_name,
                registration_ts=reg_ts,
                status=status,
                src_system="oltp",
                src_created_at=src_created_at,
                src_updated_at=src_updated_at,
            )
        )

    return pd.DataFrame(data)


# ============ STOPS ============

def generate_stops(num_stops: int) -> pd.DataFrame:
    stop_ids = [f"STOP-{i:04d}" for i in range(1, num_stops + 1)]
    base_lat, base_lon = 55.75, 37.60  # условный город
    data = []
    now = datetime.now(UTC)
    for i, stop_id in enumerate(stop_ids):
        lat = base_lat + np.random.randn() * 0.05
        lon = base_lon + np.random.randn() * 0.1
        city = "Metro City"
        zone = f"Z{random.randint(1, 5)}"
        data.append(
            dict(
                stop_id=stop_id,
                stop_code=f"SC{i+1:04d}",
                stop_name=f"Stop {i+1}",
                city=city,
                latitude=round(lat, 6),
                longitude=round(lon, 6),
                zone=zone,
                src_system="oltp",
                src_created_at=now - timedelta(days=60),
                src_updated_at=now - timedelta(days=1),
            )
        )
    return pd.DataFrame(data)


# ============ ROUTES ============

def generate_routes(num_routes: int, stops_df: pd.DataFrame, fare_products_df: pd.DataFrame):
    route_ids = [f"ROUTE-{i:03d}" for i in range(1, num_routes + 1)]
    route_types = ["bus", "tram", "metro"]
    data_routes = []
    data_route_stops = []

    stop_ids = stops_df["stop_id"].tolist()
    now = datetime.now(UTC)

    for i, route_id in enumerate(route_ids):
        route_type = random.choice(route_types)
        route_code = f"{route_type[:1].upper()}{i+1:03d}"
        route_name = f"{route_type.title()} Line {i+1}"
        description = f"Synthetic {route_type} route {i+1}"

        # Случайный набор остановок для этого маршрута
        n_stops_on_route = random.randint(10, min(25, len(stop_ids)))
        route_stop_ids = random.sample(stop_ids, n_stops_on_route)
        route_stop_ids_sorted = sorted(route_stop_ids)

        for seq, stop_id in enumerate(route_stop_ids_sorted, start=1):
            data_route_stops.append(
                dict(
                    route_id=route_id,
                    stop_id=stop_id,
                    stop_sequence=seq,
                )
            )

        pattern_hash = uuid.uuid5(uuid.NAMESPACE_DNS,
                                  ",".join(route_stop_ids_sorted)).hex

        default_fare_id = fare_products_df.sample(1)["fare_product_id"].iloc[0]

        data_routes.append(
            dict(
                route_id=route_id,
                route_code=route_code,
                route_name=route_name,
                route_type=route_type,
                description=description,
                route_pattern_hash=pattern_hash,
                default_fare_product_id=default_fare_id,
                valid_from_ts=now - timedelta(days=365),
                valid_to_ts=pd.NaT,  # datetime-колонка, а не object
                is_active=True,
                src_system="oltp",
                src_created_at=now - timedelta(days=365),
                src_updated_at=now - timedelta(days=1),
            )
        )

    routes_df = pd.DataFrame(data_routes)
    route_stops_df = pd.DataFrame(data_route_stops)
    return routes_df, route_stops_df


# ============ VEHICLES ============

def generate_vehicles(num_vehicles: int, routes_df: pd.DataFrame) -> pd.DataFrame:
    vehicle_ids = [f"VEH-{i:05d}" for i in range(1, num_vehicles + 1)]
    operators = ["CityTrans", "MetroPulse", "UrbanLines"]
    data = []
    now = datetime.now(UTC)

    route_ids = routes_df["route_id"].tolist()
    home_routes = np.random.choice(route_ids, size=num_vehicles)

    for i, vehicle_id in enumerate(vehicle_ids):
        registration_number = f"{random.randint(1000, 9999)}-{chr(65 + i % 26)}{chr(65 + (i // 26) % 26)}"
        vehicle_type = routes_df.loc[routes_df["route_id"]
                                     == home_routes[i], "route_type"].iloc[0]
        capacity = random.choice([40, 60, 80, 120])
        operator_name = random.choice(operators)

        data.append(
            dict(
                vehicle_id=vehicle_id,
                registration_number=registration_number,
                vehicle_type=vehicle_type,
                capacity=capacity,
                operator_name=operator_name,
                src_system="oltp",
                src_created_at=now - timedelta(days=200),
                src_updated_at=now - timedelta(days=1),
                # служебное поле для генерации позиций
                home_route_id=home_routes[i],
            )
        )

    df = pd.DataFrame(data)
    return df


# ============ TRIPS ============

def generate_trips(users_df: pd.DataFrame,
                   routes_df: pd.DataFrame,
                   route_stops_df: pd.DataFrame,
                   fare_products_df: pd.DataFrame,
                   start_date: date,
                   days: int) -> pd.DataFrame:
    num_users = len(users_df)
    num_trips = int(num_users * days * 1.5)

    user_ids = np.random.choice(users_df["user_id"], size=num_trips)
    route_ids = np.random.choice(routes_df["route_id"], size=num_trips)

    day_list = [start_date + timedelta(days=i) for i in range(days)]
    day_choices = np.random.choice(day_list, size=num_trips)

    start_times = []
    end_times = []
    origin_stop_ids = []
    destination_stop_ids = []
    distance_meters = []
    fare_product_ids = []
    fare_amounts = []
    currency_codes = []
    trip_statuses = []

    for i in tqdm(range(num_trips), desc="Generating trips"):
        route_id = route_ids[i]
        day = day_choices[i]
        start_base = datetime.combine(
            day, datetime.min.time()) + timedelta(hours=5)
        end_base = datetime.combine(
            day, datetime.min.time()) + timedelta(hours=23)
        start_ts = random_dates(start_base, end_base, 1)[0]

        duration_min = random.randint(5, 45)
        end_ts = start_ts + timedelta(minutes=duration_min)

        route_stops = route_stops_df[route_stops_df["route_id"] == route_id]
        if len(route_stops) < 2:
            all_stops = route_stops_df["stop_id"].unique()
            origin, dest = np.random.choice(all_stops, size=2, replace=False)
        else:
            seqs = route_stops.sort_values("stop_sequence")
            origin, dest = seqs.iloc[0]["stop_id"], seqs.iloc[-1]["stop_id"]

        n_stops_segment = random.randint(3, 15)
        dist_per_stop = random.randint(600, 1000)
        distance = n_stops_segment * dist_per_stop

        default_fare = routes_df.loc[routes_df["route_id"] == route_id,
                                     "default_fare_product_id"].iloc[0]
        fare_row = fare_products_df[fare_products_df["fare_product_id"]
                                    == default_fare].iloc[0]
        base_price = float(fare_row["base_price"])
        fare = round(max(1.0, base_price * random.uniform(0.8, 1.4)), 2)
        currency = fare_row["currency_code"]

        status = np.random.choice(["completed", "cancelled", "in_progress"],
                                  p=[0.9, 0.05, 0.05])

        start_times.append(start_ts)
        end_times.append(end_ts)
        origin_stop_ids.append(origin)
        destination_stop_ids.append(dest)
        distance_meters.append(distance)
        fare_product_ids.append(default_fare)
        fare_amounts.append(fare)
        currency_codes.append(currency)
        trip_statuses.append(status)

    created_at = [ts - timedelta(minutes=random.randint(0, 10))
                  for ts in start_times]
    updated_at = [et + timedelta(minutes=random.randint(0, 10))
                  for et in end_times]

    trip_ids = [f"TRIP-{i:08d}" for i in range(1, num_trips + 1)]

    df = pd.DataFrame(
        dict(
            trip_id=trip_ids,
            user_id=user_ids,
            route_id=route_ids,
            vehicle_id=None,
            origin_stop_id=origin_stop_ids,
            destination_stop_id=destination_stop_ids,
            start_ts=start_times,
            end_ts=end_times,
            distance_meters=distance_meters,
            fare_product_id=fare_product_ids,
            fare_amount=fare_amounts,
            currency_code=currency_codes,
            trip_status=trip_statuses,
            created_at=created_at,
            updated_at=updated_at,
            src_system="oltp",
        )
    )
    return df


# ============ PAYMENTS ============

def generate_payments(trips_df: pd.DataFrame) -> pd.DataFrame:
    completed_trips = trips_df[trips_df["trip_status"] == "completed"].copy()
    mask_paid = np.random.rand(len(completed_trips)) < 0.95
    paid_trips = completed_trips[mask_paid]

    payment_methods = ["card", "apple_pay", "google_pay", "cash"]
    data = []

    for i, row in tqdm(list(paid_trips.iterrows()), desc="Generating payments"):
        payment_id = f"PAY-{i:08d}"
        payment_method = random.choice(payment_methods)
        amount = row["fare_amount"]
        currency = row["currency_code"]
        status = "success"
        created_at = row["start_ts"] - timedelta(minutes=random.randint(0, 5))
        updated_at = created_at + timedelta(minutes=random.randint(0, 5))
        external_txn_id = uuid.uuid4().hex

        data.append(
            dict(
                payment_id=payment_id,
                user_id=row["user_id"],
                fare_product_id=row["fare_product_id"],
                payment_method=payment_method,
                amount=amount,
                currency_code=currency,
                payment_status=status,
                external_txn_id=external_txn_id,
                created_at=created_at,
                updated_at=updated_at,
                src_system="oltp",
            )
        )

    return pd.DataFrame(data)


# ============ VEHICLE POSITIONS ============

def generate_vehicle_positions(vehicles_df: pd.DataFrame,
                               stops_df: pd.DataFrame,
                               route_stops_df: pd.DataFrame,
                               start_date: date,
                               days: int,
                               freq_seconds: int) -> pd.DataFrame:
    events = []

    stop_coords = stops_df.set_index(
        "stop_id")[["latitude", "longitude"]].to_dict(orient="index")

    route_centers = {}
    for route_id, group in route_stops_df.groupby("route_id"):
        lats, lons = [], []
        for stop_id in group["stop_id"]:
            coord = stop_coords.get(stop_id)
            if coord:
                lats.append(coord["latitude"])
                lons.append(coord["longitude"])
        if lats:
            route_centers[route_id] = (
                float(np.mean(lats)), float(np.mean(lons)))
        else:
            route_centers[route_id] = (55.75, 37.60)

    partition_choices = list(range(4))

    for _, veh in tqdm(vehicles_df.iterrows(), total=len(vehicles_df), desc="Generating vehicle positions (per vehicle)"):
        vehicle_id = veh["vehicle_id"]
        route_id = veh["home_route_id"]
        center_lat, center_lon = route_centers.get(route_id, (55.75, 37.60))

        for d in range(days):
            day = start_date + timedelta(days=d)
            start_ts = datetime.combine(
                day, datetime.min.time()) + timedelta(hours=5)
            end_ts = datetime.combine(
                day, datetime.min.time()) + timedelta(hours=23)

            times = pd.date_range(start_ts, end_ts, freq=f"{freq_seconds}s")
            if len(times) == 0:
                continue

            lat_noise = np.cumsum(np.random.randn(len(times)) * 0.0005)
            lon_noise = np.cumsum(np.random.randn(len(times)) * 0.0008)

            speeds = np.clip(np.random.normal(
                loc=30, scale=10, size=len(times)), 0, 70)
            headings = np.mod(np.random.normal(
                loc=90, scale=60, size=len(times)), 360)

            nearest_stops = route_stops_df[route_stops_df["route_id"]
                                           == route_id]["stop_id"].tolist()
            if not nearest_stops:
                nearest_stops = stops_df["stop_id"].tolist()

            for idx, ts in enumerate(times):
                events.append(
                    dict(
                        raw_event_id=f"{vehicle_id}-{int(ts.timestamp())}",
                        vehicle_id=vehicle_id,
                        route_id=route_id,
                        event_ts=ts.to_pydatetime(),
                        latitude=round(center_lat + lat_noise[idx], 6),
                        longitude=round(center_lon + lon_noise[idx], 6),
                        speed_kmph=round(float(speeds[idx]), 2),
                        heading_deg=round(float(headings[idx]), 2),
                        nearest_stop_id=random.choice(nearest_stops),
                        partition_id=random.choice(partition_choices),
                        offset_in_partition=random.randint(0, 10_000_000),
                        src_system="kafka_vehicle_positions",
                    )
                )

    df = pd.DataFrame(events)
    return df


# ============ IO ============

def write_parquet(df: pd.DataFrame, out_dir: Path, name: str):
    file_path = out_dir / f"{name}.parquet"
    df.to_parquet(file_path, index=False)
    print(f"Wrote {len(df):,} rows to {file_path}")


def main():
    args = parse_args()
    random.seed(args.seed)
    np.random.seed(args.seed)

    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    else:
        start_date = (datetime.now(UTC) - timedelta(days=args.days)).date()

    output_dir = Path(args.output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)

    print("Generating reference data...")
    fare_products_df = generate_fare_products()
    users_df = generate_users(args.num_users, start_date, args.days)
    stops_df = generate_stops(args.num_stops)
    routes_df, route_stops_df = generate_routes(
        args.num_routes, stops_df, fare_products_df)
    vehicles_df = generate_vehicles(args.num_vehicles, routes_df)

    print("Generating facts...")
    trips_df = generate_trips(users_df, routes_df, route_stops_df, fare_products_df,
                              start_date, args.days)
    payments_df = generate_payments(trips_df)
    vehicle_positions_df = generate_vehicle_positions(
        vehicles_df, stops_df, route_stops_df,
        start_date, args.days,
        freq_seconds=10,
    )

    # убираем служебную колонку перед сохранением в stg_vehicle
    vehicles_to_write = vehicles_df.drop(columns=["home_route_id"])

    print("Writing Parquet files...")
    write_parquet(fare_products_df, output_dir, "fare_products")
    write_parquet(users_df, output_dir, "users")
    write_parquet(stops_df, output_dir, "stops")
    write_parquet(routes_df, output_dir, "routes")
    write_parquet(vehicles_to_write, output_dir, "vehicles")
    write_parquet(trips_df, output_dir, "trips")
    write_parquet(payments_df, output_dir, "payments")
    write_parquet(vehicle_positions_df, output_dir, "vehicle_positions")

    minio.push_to_minio(output_dir, "http://localhost:9000",
                        "metropulse-raw", ("minio", "minio123456"))


if __name__ == "__main__":
    main()
