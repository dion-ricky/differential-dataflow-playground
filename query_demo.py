"""Query the generated order model with DuckDB SQL over the Delta tables.

Prefers DuckDB's native Delta extension (`delta_scan`); if the extension can't
be installed (offline), falls back to registering the Delta tables as Arrow
views, which gives identical query results.

    .venv/bin/python query_demo.py
"""

from __future__ import annotations

import duckdb

from deltalake import DeltaTable
from schema import table_path

TABLES = ("users", "orders", "order_items")


def connect() -> tuple[duckdb.DuckDBPyConnection, str]:
    con = duckdb.connect()
    try:
        con.execute("INSTALL delta; LOAD delta;")
        return con, "delta_scan (native Delta extension)"
    except Exception as exc:  # offline / extension unavailable
        con = duckdb.connect()
        for name in TABLES:
            arrow = DeltaTable(str(table_path(name))).to_pyarrow_table()
            con.register(name, arrow)
        return con, f"arrow views (delta extension unavailable: {str(exc).splitlines()[0][:60]})"


def q(con: duckdb.DuckDBPyConnection, title: str, sql: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    df = con.execute(sql).df()
    print(df.to_string(index=False) if len(df) else "  <no rows>")


def main() -> None:
    con, mode = connect()
    using_delta = mode.startswith("delta_scan")

    def scan(name: str) -> str:
        return f"delta_scan('{table_path(name)}')" if using_delta else name

    print(f"reading Delta tables via {mode}")

    q(
        con,
        "1. revenue by order status (orders x items join)",
        f"""
        SELECT o.status,
               count(DISTINCT o.order_id) AS orders,
               sum(i.quantity)            AS units,
               round(sum(i.line_total), 2) AS revenue
        FROM {scan("orders")} o
        JOIN {scan("order_items")} i USING (order_id)
        GROUP BY o.status
        ORDER BY revenue DESC
        """,
    )

    q(
        con,
        "2. top products by revenue",
        f"""
        SELECT i.sku,
               any_value(i.product_name)    AS product,
               sum(i.quantity)              AS units,
               round(sum(i.line_total), 2)  AS revenue
        FROM {scan("order_items")} i
        GROUP BY i.sku
        ORDER BY revenue DESC
        LIMIT 8
        """,
    )

    q(
        con,
        "3. highest-value customers (users x orders x items)",
        f"""
        SELECT u.user_id, u.full_name, u.country,
               count(DISTINCT o.order_id)   AS orders,
               round(sum(i.line_total), 2)  AS lifetime_value
        FROM {scan("users")} u
        JOIN {scan("orders")} o USING (user_id)
        JOIN {scan("order_items")} i USING (order_id)
        GROUP BY u.user_id, u.full_name, u.country
        ORDER BY lifetime_value DESC
        LIMIT 8
        """,
    )

    q(
        con,
        "4. daily order volume with 7-day rolling total",
        f"""
        WITH daily AS (
          SELECT order_date, count(*) AS orders
          FROM {scan("orders")}
          GROUP BY order_date
        )
        SELECT order_date, orders,
               sum(orders) OVER (
                 ORDER BY order_date
                 ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
               ) AS rolling_7d
        FROM daily
        ORDER BY order_date DESC
        LIMIT 10
        """,
    )

    q(
        con,
        "5. integrity sweep (both must be 0)",
        f"""
        SELECT
          (SELECT count(*) FROM {scan("order_items")} i
             LEFT JOIN {scan("orders")} o USING (order_id)
            WHERE o.order_id IS NULL) AS orphan_items,
          (SELECT count(*) FROM {scan("orders")} o
             LEFT JOIN {scan("users")} u USING (user_id)
            WHERE u.user_id IS NULL) AS orphan_orders
        """,
    )


if __name__ == "__main__":
    main()
