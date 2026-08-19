"""
AtliQ Commerce — Daily Transaction Simulator
============================================
Purpose: keep the OLTP database "alive" so the nightly OLTP -> OLAP sync has
new/changed rows to pick up. Run it once a day (or a few times) before the
nightly pipeline. It inserts a handful of fresh orders for random existing
customers, using existing products, and records a payment for most of them.

This is what makes the incremental (watermark) sync feel real: every run
bumps updated_at on new rows, and the pipeline should pick up ONLY those.

Prereqs:
    pip install pyodbc python-dotenv
    ODBC Driver 18 for SQL Server installed.
Set these environment variables (or use a .env file):
    AZ_SQL_SERVER   e.g. atliq-sql.database.windows.net
    AZ_SQL_DB       e.g. atliq_commerce
    AZ_SQL_USER     e.g. atliq_admin
    AZ_SQL_PASSWORD your password
"""
import os
import random
import argparse
from datetime import datetime, date

import pyodbc
from dotenv import load_dotenv

load_dotenv()

STATUSES = ["Placed", "Placed", "Shipped", "Delivered"]
METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet", "COD"]


def get_conn():
    server = os.environ["AZ_SQL_SERVER"]
    database = os.environ["AZ_SQL_DB"]
    user = os.environ["AZ_SQL_USER"]
    password = os.environ["AZ_SQL_PASSWORD"]
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};DATABASE={database};UID={user};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def simulate(n_orders: int):
    conn = get_conn()
    cur = conn.cursor()

    # pull existing ids so every FK is valid
    customer_ids = [r[0] for r in cur.execute("SELECT customer_id FROM dbo.customers").fetchall()]
    products = cur.execute("SELECT product_id, unit_price FROM dbo.products").fetchall()

    today = date.today()
    now = datetime.now()
    created_orders = 0

    for _ in range(n_orders):
        customer_id = random.choice(customer_ids)
        status = random.choice(STATUSES)

        # 1) insert order header (amount filled in after items)
        cur.execute(
            """INSERT INTO dbo.orders (customer_id, order_date, status, order_amount, created_at, updated_at)
                   OUTPUT INSERTED.order_id
                   VALUES (?, ?, ?, 0, ?, ?)""",
            customer_id, today, status, now, now,
        )
        order_id = cur.fetchone()[0]

        # 2) 1-4 line items
        order_total = 0
        for p in random.sample(products, random.randint(1, 4)):
            product_id, unit_price = int(p[0]), float(p[1])
            qty = random.randint(1, 3)
            cur.execute(
                """INSERT INTO dbo.order_items (order_id, product_id, quantity, item_price, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
                order_id, product_id, qty, unit_price, now,
            )
            order_total += qty * unit_price

        # 3) update header amount
        cur.execute("UPDATE dbo.orders SET order_amount = ?, updated_at = ? WHERE order_id = ?",
                    order_total, now, order_id)

        # 4) payment for non-cancelled
        if status != "Cancelled":
            cur.execute(
                """INSERT INTO dbo.payments (order_id, amount, method, paid_at, updated_at)
                       VALUES (?, ?, ?, ?, ?)""",
                order_id, order_total, random.choice(METHODS), now, now,
            )
        created_orders += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"[{now:%Y-%m-%d %H:%M}] Simulated {created_orders} new orders.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Insert simulated daily orders into AtliQ OLTP.")
    ap.add_argument("--orders", type=int, default=8, help="how many orders to create this run")
    args = ap.parse_args()
    simulate(args.orders)
