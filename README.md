# Delta Lake playground — order model + fake data

A local, JVM-free Delta Lake setup ([delta-rs](https://github.com/delta-io/delta-rs))
with a three-table order model, a deterministic fake data generator, and DuckDB
SQL on top. No Spark, no Java, no cloud storage.

## Files

| File | What it does |
|---|---|
| `schema.py` | Arrow schemas + partition config for the three tables (single source of truth) |
| `generate_fake_data.py` | generates `users` → `orders` → `order_items`, verifies FKs/totals/PKs |
| `query_demo.py` | DuckDB `delta_scan` queries (joins, rollups, windows, integrity sweep) |
| `play_delta.py` | tour of Delta ops on a **separate** demo table (`data/demo_orders`) |
| `data/` | the Delta tables (gitignored) |

## Quickstart

```bash
cd /home/opencode/ice

.venv/bin/python generate_fake_data.py                    # 500 users, 5k orders, ~12k items
.venv/bin/python query_demo.py                            # SQL over all three tables
.venv/bin/python play_delta.py                            # create/merge/delete/time-travel tour
```

Regenerate deterministically, scale it, or add more rows:

```bash
.venv/bin/python generate_fake_data.py --seed 7 --reset
.venv/bin/python generate_fake_data.py --users 5_000 --orders 200_000 --days 365
.venv/bin/python generate_fake_data.py --append --orders 1_000 --days 7
```

Generator flags: `--users --orders --days --items-min --items-max --seed --append --reset`.
Every run ends with `integrity ok: ...` or exits non-zero on a broken invariant.

## Model

```
users 1 ──── * orders 1 ──── * order_items
```

**`data/users`** — one row per account, unpartitioned

| column | type |
|---|---|
| `user_id` | int64 (PK) |
| `email`, `full_name`, `country`, `currency` | string |
| `signup_at` | timestamp[us, UTC] |
| `is_active` | bool |

**`data/orders`** — order headers, **partitioned by `order_date`** (`order_date=YYYY-MM-DD/`)

| column | type |
|---|---|
| `order_id` | int64 (PK) |
| `user_id` | int64 (FK → users) |
| `status` | string: pending / paid / shipped / delivered / cancelled / refunded |
| `currency` | string, inherited from the user's country |
| `order_ts`, `updated_at` | timestamp[us, UTC] |
| `order_date` | string (partition key) |

**`data/order_items`** — line items, unpartitioned

| column | type |
|---|---|
| `order_item_id` | int64 (PK) |
| `order_id` | int64 (FK → orders) |
| `sku`, `product_name` | string (12-product catalog) |
| `quantity` | int32 (1–3) |
| `unit_price` | float64 (base price ±10%) |
| `discount_pct` | float64 (0 / 5 / 10 / 15 / 25%) |
| `line_total` | float64 = `quantity * unit_price * (1 - discount_pct)`, rounded to 2dp |
| `created_at` | timestamp[us, UTC] |

`order_items` is intentionally *not* partitioned: the date already lives on the
parent order, and a flat child table keeps joins simple. Partitioning it by
`order_date` would duplicate the key — a fine choice at larger scale, but noise
at this size.

## One-liners

```python
from deltalake import DeltaTable, write_deltalake

dt = DeltaTable("data/orders")
dt.to_pandas()                                            # read
dt.version(), dt.history(limit=5)                         # versions
DeltaTable("data/orders", version=0).to_pandas()          # time travel
dt.to_pyarrow_table(filters=[("status", "=", "delivered")])  # pushdown
dt.optimize.compact()                                     # small-file compaction
dt.vacuum(dry_run=True)                                   # unreferenced files
```

SQL straight at the Delta log, no registration step:

```python
import duckdb
duckdb.sql("INSTALL delta; LOAD delta;")
duckdb.sql("SELECT status, count(*) FROM delta_scan('data/orders') GROUP BY 1").show()
```

## Gotchas worth knowing

- **`vacuum` retention.** Default is 7 days, so a fresh table frees nothing.
  `play_delta.py` lowers retention to 0 with `dry_run=True` purely to *show*
  the stale files. Vacuuming for real breaks older time-travel reads.
- **Deletes are logical until vacuum.** `delete` writes tombstones plus new
  Parquet files; old files stay until vacuumed.
- **Append keeps ids contiguous.** `--append` reads the current max `order_id` /
  `order_item_id` and continues from there, so no PK collisions.
- **Seed determinism covers ids and rows**, but `Faker.seed()` is global; if you
  generate twice in one process you'd get different names. Run the script per
  dataset, or use `--append` for incremental growth.
- **No multi-writer concurrency.** delta-rs uses file locks; one local process
  at a time is fine, parallel writers on local disk are not.
- **Currency is nominal.** Prices are not FX-converted; `currency` is a label
  inherited from the user, not a unit conversion.
