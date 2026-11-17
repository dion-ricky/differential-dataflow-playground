"""Fake data generator for the local Delta order model.

Generates users -> orders -> order_items with stable ids, valid foreign keys
and line totals that match their parent rows.

    .venv/bin/python generate_fake_data.py                       # 500 users, 5k orders
    .venv/bin/python generate_fake_data.py --users 50 --orders 300 --days 30
    .venv/bin/python generate_fake_data.py --seed 7 --append      # add more orders
    .venv/bin/python generate_fake_data.py --reset                # wipe and regenerate

Design notes:
- Deterministic for a given --seed: same seed => same ids, same rows.
- `--append` keeps existing users and only adds order headers/items, so the
  tables stay referentially valid across runs.
- Timestamps are UTC; `order_date` is the partition key derived from order_ts.
"""

from __future__ import annotations

import argparse
import random
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from faker import Faker

from schema import (
    DATA_DIR,
    ORDERS_SCHEMA,
    PARTITIONS,
    DESCRIPTIONS,
    ITEMS_SCHEMA,
    USERS_SCHEMA,
    empty_table,
    table_path,
    validate_columns,
)

# (sku, product name, base price)
CATALOG: list[tuple[str, str, float]] = [
    ("SKU-KEY-01", "Mechanical Keyboard", 129.00),
    ("SKU-MOU-02", "Ergonomic Mouse", 59.50),
    ("SKU-MON-03", "27in Monitor", 319.00),
    ("SKU-HUB-04", "USB-C Hub", 45.00),
    ("SKU-CAM-05", "1080p Webcam", 89.00),
    ("SKU-HED-06", "Noise-cancelling Headphones", 249.00),
    ("SKU-STD-07", "Laptop Stand", 39.00),
    ("SKU-CAB-08", "USB-C Cable 2m", 12.00),
    ("SKU-SSD-09", "1TB NVMe SSD", 109.00),
    ("SKU-MIC-10", "Condenser Microphone", 149.00),
    ("SKU-CHR-11", "Office Chair", 429.00),
    ("SKU-DSK-12", "Standing Desk", 599.00),
]

COUNTRIES: list[tuple[str, str]] = [
    ("US", "USD"),
    ("GB", "GBP"),
    ("DE", "EUR"),
    ("FR", "EUR"),
    ("NL", "EUR"),
    ("BR", "BRL"),
    ("IN", "INR"),
    ("JP", "JPY"),
    ("CA", "CAD"),
    ("AU", "AUD"),
]

# Status distribution; weights sum to 1.0.
STATUS_WEIGHTS: list[tuple[str, float]] = [
    ("delivered", 0.40),
    ("shipped", 0.20),
    ("paid", 0.20),
    ("pending", 0.10),
    ("cancelled", 0.07),
    ("refunded", 0.03),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--users", type=int, default=500, help="users to create (ignored with --append when users exist)")
    p.add_argument("--orders", type=int, default=5_000, help="order headers to create")
    p.add_argument("--days", type=int, default=90, help="spread orders over the last N days")
    p.add_argument("--items-min", type=int, default=1, help="minimum line items per order")
    p.add_argument("--items-max", type=int, default=4, help="maximum line items per order")
    p.add_argument("--seed", type=int, default=42, help="RNG seed (deterministic output)")
    p.add_argument("--append", action="store_true", help="append orders/items instead of resetting")
    p.add_argument("--reset", action="store_true", help="delete the three tables first (default)")
    return p.parse_args()


def now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def build_users(n: int, rng: random.Random, fake: Faker, start_id: int = 1) -> list[dict]:
    rows: list[dict] = []
    horizon = now()
    for i in range(n):
        country, currency = rng.choice(COUNTRIES)
        # Always sign up before the order window starts, so FK timestamps are sane.
        signup = horizon - timedelta(days=rng.uniform(30, 730))
        rows.append(
            {
                "user_id": start_id + i,
                "email": fake.unique.email(),
                "full_name": fake.name(),
                "country": country,
                "currency": currency,
                "signup_at": signup,
                "is_active": rng.random() > 0.08,
            }
        )
    return rows


def pick_status(rng: random.Random) -> str:
    r, acc = rng.random(), 0.0
    for status, weight in STATUS_WEIGHTS:
        acc += weight
        if r <= acc:
            return status
    return STATUS_WEIGHTS[-1][0]


def build_orders(
    users: list[dict],
    n_orders: int,
    days: int,
    rng: random.Random,
    start_order_id: int,
    start_item_id: int,
    items_min: int,
    items_max: int,
) -> tuple[list[dict], list[dict]]:
    orders: list[dict] = []
    items: list[dict] = []
    end = now()
    start = end - timedelta(days=days)
    user_currency = {u["user_id"]: u["currency"] for u in users}

    for i in range(n_orders):
        order_id = start_order_id + i
        user_id = rng.choice(users)["user_id"]
        order_ts = start + timedelta(seconds=rng.uniform(0, (end - start).total_seconds()))
        order_ts = order_ts.replace(microsecond=0)
        status = pick_status(rng)

        orders.append(
            {
                "order_id": order_id,
                "user_id": user_id,
                "status": status,
                "currency": user_currency[user_id],
                "order_ts": order_ts,
                "order_date": order_ts.date().isoformat(),
                "updated_at": order_ts + timedelta(minutes=rng.randint(1, 2880)),
            }
        )

        for line in range(rng.randint(items_min, items_max)):
            sku, name, base_price = rng.choice(CATALOG)
            qty = rng.choices([1, 1, 1, 2, 2, 3], k=1)[0]
            # Slight per-line price drift, then a small discount.
            unit_price = round(base_price * rng.uniform(0.9, 1.1), 2)
            discount = rng.choice([0.0, 0.0, 0.0, 0.05, 0.1, 0.15, 0.25])
            line_total = round(qty * unit_price * (1 - discount), 2)
            items.append(
                {
                    "order_item_id": start_item_id + len(items),
                    "order_id": order_id,
                    "sku": sku,
                    "product_name": name,
                    "quantity": qty,
                    "unit_price": unit_price,
                    "discount_pct": discount,
                    "line_total": line_total,
                    "created_at": order_ts,
                }
            )
    return orders, items


def write_table(name: str, rows: list[dict], schema: pa.Schema, mode: str) -> DeltaTable:
    table = pa.Table.from_pylist(rows, schema=schema) if rows else empty_table(name)
    validate_columns(name, table)
    write_deltalake(
        str(table_path(name)),
        table,
        mode=mode,
        partition_by=PARTITIONS[name] or None,
        name=name,
        description=DESCRIPTIONS[name],
    )
    return DeltaTable(str(table_path(name)))


def load_existing_users() -> list[dict]:
    dt = DeltaTable(str(table_path("users")))
    return dt.to_pyarrow_table().to_pylist()


def existing_id_limits() -> tuple[int, int]:
    """Highest ids already present, so appended rows never collide."""
    max_order = max_item = 0
    if table_path("orders").exists():
        orders = DeltaTable(str(table_path("orders"))).to_pyarrow_table()
        max_order = max(orders["order_id"].to_pylist(), default=0)
    if table_path("order_items").exists():
        items = DeltaTable(str(table_path("order_items"))).to_pyarrow_table()
        max_item = max(items["order_item_id"].to_pylist(), default=0)
    return max_order, max_item


def verify() -> None:
    """Referential integrity + arithmetic checks over what's on disk."""
    users = DeltaTable(str(table_path("users"))).to_pyarrow_table().to_pandas()
    orders = DeltaTable(str(table_path("orders"))).to_pyarrow_table().to_pandas()
    items = DeltaTable(str(table_path("order_items"))).to_pyarrow_table().to_pandas()

    user_ids = set(users["user_id"])
    order_ids = set(orders["order_id"])

    orphan_orders = set(orders["user_id"]) - user_ids
    orphan_items = set(items["order_id"]) - order_ids
    duplicates = orders["order_id"].duplicated().sum() + items["order_item_id"].duplicated().sum()

    expected = (items["quantity"] * items["unit_price"] * (1 - items["discount_pct"])).round(2)
    bad_totals = int((expected - items["line_total"]).abs().gt(0.011).sum())

    if orphan_orders:
        raise SystemExit(f"FAIL: {len(orphan_orders)} orders reference missing users: {sorted(orphan_orders)[:5]}")
    if orphan_items:
        raise SystemExit(f"FAIL: {len(orphan_items)} items reference missing orders: {sorted(orphan_items)[:5]}")
    if duplicates:
        raise SystemExit(f"FAIL: {duplicates} duplicate primary keys")
    if bad_totals:
        raise SystemExit(f"FAIL: {bad_totals} line_total values disagree with quantity*price*(1-discount)")

    print(
        f"integrity ok: {len(users):,} users, {len(orders):,} orders, {len(items):,} items "
        f"| fk + totals + PKs verified"
    )


def main() -> None:
    args = parse_args()
    if args.items_min < 1 or args.items_max < args.items_min:
        raise SystemExit("--items-min must be >= 1 and <= --items-max")

    append = args.append and not args.reset
    if not append:
        for name in ("users", "orders", "order_items"):
            if table_path(name).exists():
                shutil.rmtree(table_path(name))
        print(f"reset: removed existing tables under {DATA_DIR}")

    rng = random.Random(args.seed)
    fake = Faker()
    Faker.seed(args.seed)

    if append and table_path("users").exists():
        users = load_existing_users()
        start_order_id, start_item_id = existing_id_limits()
        start_order_id += 1
        start_item_id += 1
        print(f"append: reusing {len(users):,} existing users, next order_id={start_order_id}")
    else:
        start_order_id = start_item_id = 1
        users = build_users(args.users, rng, fake)
        write_table("users", users, USERS_SCHEMA, mode="overwrite")
        print(f"users:     {len(users):,} rows -> {table_path('users')}")

    orders, items = build_orders(
        users,
        args.orders,
        args.days,
        rng,
        start_order_id,
        start_item_id,
        args.items_min,
        args.items_max,
    )

    write_table("orders", orders, ORDERS_SCHEMA, mode="append" if append else "overwrite")
    write_table("order_items", items, ITEMS_SCHEMA, mode="append" if append else "overwrite")
    print(f"orders:    {len(orders):,} rows -> {table_path('orders')}")
    print(f"items:     {len(items):,} rows -> {table_path('order_items')}")

    verify()

    print("\ntable state:")
    for name in ("users", "orders", "order_items"):
        dt = DeltaTable(str(table_path(name)))
        size = sum(f.stat().st_size for f in table_path(name).rglob("*") if f.is_file())
        print(
            f"  {name:<11} version={dt.version():<2} rows={dt.count():>7,} "
            f"files={len(dt.file_uris()):>3} partitions={dt.metadata().partition_columns} "
            f"disk={size / 1024:.0f} KiB"
        )


if __name__ == "__main__":
    main()
