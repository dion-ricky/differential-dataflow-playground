"""Shared schema definition for the local order model.

Three Delta tables, all under ./data:

    users        (user_id PK)                     -- not partitioned
    orders       (order_id PK, user_id FK)        -- partitioned by order_date
    order_items  (order_item_id PK, order_id FK)  -- not partitioned

Everything is Arrow-first so the generator and any reader agree exactly on
physical types (no pandas dtype drift).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

DATA_DIR = Path(__file__).resolve().parent / "data"

USERS_SCHEMA = pa.schema(
    [
        ("user_id", pa.int64()),
        ("email", pa.string()),
        ("full_name", pa.string()),
        ("country", pa.string()),
        ("currency", pa.string()),
        ("signup_at", pa.timestamp("us", tz="UTC")),
        ("is_active", pa.bool_()),
    ]
)

ORDERS_SCHEMA = pa.schema(
    [
        ("order_id", pa.int64()),
        ("user_id", pa.int64()),
        ("status", pa.string()),
        ("currency", pa.string()),
        ("order_ts", pa.timestamp("us", tz="UTC")),
        ("order_date", pa.string()),  # partition column: YYYY-MM-DD
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)

ITEMS_SCHEMA = pa.schema(
    [
        ("order_item_id", pa.int64()),
        ("order_id", pa.int64()),
        ("sku", pa.string()),
        ("product_name", pa.string()),
        ("quantity", pa.int32()),
        ("unit_price", pa.float64()),
        ("discount_pct", pa.float64()),
        ("line_total", pa.float64()),
        ("created_at", pa.timestamp("us", tz="UTC")),
    ]
)

SCHEMAS: dict[str, pa.Schema] = {
    "users": USERS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ITEMS_SCHEMA,
}

# Table -> partition columns. orders is partitioned for pruning demos; the
# child table stays flat so a join never has to reconcile two date layouts.
PARTITIONS: dict[str, list[str]] = {
    "users": [],
    "orders": ["order_date"],
    "order_items": [],
}

DESCRIPTIONS: dict[str, str] = {
    "users": "fake users (one row per account)",
    "orders": "fake orders (one row per order header)",
    "order_items": "fake order line items (one row per product line)",
}


def table_path(name: str) -> Path:
    if name not in SCHEMAS:
        raise KeyError(f"unknown table {name!r}; expected one of {sorted(SCHEMAS)}")
    return DATA_DIR / name


def empty_table(name: str) -> pa.Table:
    return SCHEMAS[name].empty_table()


def validate_columns(name: str, table: pa.Table) -> None:
    """Fail fast if a caller hands over the wrong columns/order."""
    expected = SCHEMAS[name]
    if table.schema.names != expected.names:
        raise ValueError(
            f"{name}: column mismatch\n  expected {expected.names}\n  got      {table.schema.names}"
        )
