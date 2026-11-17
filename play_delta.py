"""Local Delta Lake playground (delta-rs, no Spark/JVM required).

Walks through the core operations on a standalone demo table at
./data/demo_orders:
create -> append -> update -> MERGE upsert -> delete -> query ->
time travel -> history/log inspection -> compaction + vacuum dry-run.

The order model in ./data/{users,orders,order_items} is generated separately by
generate_fake_data.py; this script deliberately uses its own table so the two
never collide.

Run:  .venv/bin/python play_delta.py
Reset anytime by deleting ./data/demo_orders (the script also recreates it).
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

TABLE = Path(__file__).parent / "data" / "demo_orders"

SCHEMA = pa.schema(
    [
        ("order_id", pa.int64()),
        ("customer", pa.string()),
        ("amount", pa.float64()),
        ("status", pa.string()),
        ("order_date", pa.string()),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)


def rows(data: list[tuple]) -> pa.Table:
    """Build an Arrow table from tuples, enforcing the shared schema."""
    records = []
    for r in data:
        record = dict(zip([f.name for f in SCHEMA], r))
        record["updated_at"] = datetime.fromisoformat(record["updated_at"])
        records.append(record)
    return pa.Table.from_pylist(records, schema=SCHEMA)


def show(label: str, table: DeltaTable | pa.Table) -> None:
    arrow = table.to_pyarrow_table() if isinstance(table, DeltaTable) else table
    df = arrow.to_pandas()
    print(f"\n{label}  ({arrow.num_rows} rows)")
    if arrow.num_rows:
        print(df.to_string(index=False))
    else:
        print("  <empty>")


def header(step: int, title: str) -> None:
    print("\n" + "=" * 72)
    print(f"STEP {step}: {title}")
    print("=" * 72)


# ---------------------------------------------------------------- reset
if TABLE.exists():
    shutil.rmtree(TABLE)
print(f"Table location: {TABLE}")

# ---------------------------------------------------------------- 1. create
header(1, "CREATE (initial write, partitioned by order_date)")
initial = rows(
    [
        (1001, "ada", 42.50, "new", "2026-11-10", "2026-11-10T09:00:00Z"),
        (1002, "grace", 17.00, "new", "2026-11-10", "2026-11-10T09:05:00Z"),
        (1003, "linus", 99.99, "paid", "2026-11-10", "2026-11-10T10:15:00Z"),
        (1004, "ada", 5.25, "paid", "2026-11-11", "2026-11-11T08:30:00Z"),
        (1005, "barbara", 250.00, "cancelled", "2026-11-11", "2026-11-11T11:45:00Z"),
    ]
)
write_deltalake(
    TABLE,
    initial,
    mode="error",
    partition_by=["order_date"],
    name="orders",
    description="playground orders table",
)
dt = DeltaTable(TABLE)
print(f"version={dt.version()}  files={len(dt.file_uris())}")
show("current contents:", dt)

# ---------------------------------------------------------------- 2. append
header(2, "APPEND (late-arriving rows for an existing partition + a new day)")
late = rows(
    [
        (1006, "alan", 12.00, "new", "2026-11-10", "2026-11-12T07:00:00Z"),
        (1007, "edsger", 64.00, "paid", "2026-11-12", "2026-11-12T07:30:00Z"),
        (1008, "grace", 3.99, "new", "2026-11-12", "2026-11-12T08:00:00Z"),
    ]
)
write_deltalake(TABLE, late, mode="append")
dt = DeltaTable(TABLE)
print(f"version={dt.version()}  rows={dt.count()}  files={len(dt.file_uris())}")

# ---------------------------------------------------------------- 3. update
header(3, "UPDATE (predicate-based, no rewrite of untouched files)")
dt.update(
    predicate="order_id = 1002",
    new_values={
        "status": "shipped",
        "updated_at": datetime.fromisoformat("2026-11-12T09:00:00+00:00"),
    },
)
dt = DeltaTable(TABLE)
print(f"version={dt.version()}")
show("order 1002:", DeltaTable(TABLE).to_pyarrow_table(filters=[("order_id", "=", 1002)]))

# ---------------------------------------------------------------- 4. merge
header(4, "MERGE / UPSERT (update an existing row, insert a brand-new one)")
upserts = rows(
    [
        (1004, "ada", 8.75, "shipped", "2026-11-11", "2026-11-12T10:00:00Z"),
        (2001, "margaret", 120.00, "new", "2026-11-12", "2026-11-12T10:05:00Z"),
    ]
)
(
    DeltaTable(TABLE)
    .merge(upserts, predicate="t.order_id = s.order_id", source_alias="s", target_alias="t")
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute()
)
dt = DeltaTable(TABLE)
print(f"version={dt.version()}  rows={dt.count()}")

# ---------------------------------------------------------------- 5. delete
header(5, "DELETE (drop cancelled order 1005)")
DeltaTable(TABLE).delete(predicate="order_id = 1005")
dt = DeltaTable(TABLE)
print(f"version={dt.version()}  rows={dt.count()}")
show("after delete:", dt)

# ---------------------------------------------------------------- 6. query
header(6, "QUERY (partition pruning + aggregation)")
show(
    "orders on 2026-11-10:",
    DeltaTable(TABLE).to_pyarrow_table(filters=[("order_date", "=", "2026-11-10")]),
)

agg = (
    dt.to_pyarrow_table()
    .to_pandas()
    .groupby(["order_date", "status"], as_index=False)
    .agg(orders=("order_id", "count"), revenue=("amount", "sum"))
)
print("\nrevenue by date/status:")
print(agg.to_string(index=False))

# ---------------------------------------------------------------- 7. time travel
header(7, "TIME TRAVEL (read the table as it was at earlier versions)")
for version in (0, 2):
    snap = DeltaTable(TABLE, version=version)
    print(
        f"version {version}: rows={snap.to_pyarrow_table().num_rows} "
        f"statuses={sorted(set(snap.to_pyarrow_table()['status'].to_pylist()))}"
    )

# ---------------------------------------------------------------- 8. inspect
header(8, "INSPECT (history, protocol, physical layout)")
print("\nhistory (newest first):")
for h in dt.history():
    ts = datetime.fromtimestamp(h["timestamp"] / 1000, tz=timezone.utc)
    print(f"  v{h['version']:<2} {ts:%Y-%m-%d %H:%M:%S}  {h.get('operation', '?')}")

print(f"\nprotocol: {dt.protocol()}")
print(f"partitions: {dt.metadata().partition_columns}")
print("data files:")
for uri in sorted(dt.file_uris()):
    print(f"  {uri.split('/data/')[-1]}")

print("\n_delta_log commits:")
for log in sorted((TABLE / "_delta_log").glob("*.json")):
    actions = [json.loads(line) for line in log.read_text().splitlines() if line.strip()]
    kinds = [next(iter(a)) for a in actions]
    print(f"  {log.name}: {len(actions)} actions {kinds}")

# ---------------------------------------------------------------- 9. maintenance
header(9, "MAINTENANCE (compact small files, then dry-run vacuum)")
print(f"files before compaction: {len(dt.file_uris())}")
metrics = dt.optimize.compact()
print(f"compaction result: {metrics}")
print(f"files after compaction: {len(DeltaTable(TABLE).file_uris())}")

stale = DeltaTable(TABLE).vacuum(
    retention_hours=0, enforce_retention_duration=False, dry_run=True
)
print(f"vacuum would remove {len(stale)} unreferenced file(s) (dry run, nothing deleted)")
for f in stale:
    print(f"  {f.split('/data/')[-1]}")

print("\nDone. Table at ./data/demo_orders — inspect ./data/demo_orders/_delta_log/*.json")
