
from fastapi import APIRouter
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import random

DB_URL = "postgresql://laravel_user:>V4H?Q!6PZwXw$+C@161.35.143.76:5432/productionDev"

engine = create_engine(
    DB_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)

SessionLocal = sessionmaker(bind=engine)
app = APIRouter()


# -----------------------------
# Helper
# -----------------------------
def is_valid_position(lat, lng):
    return lat is not None and lng is not None


@app.get("/salesmen")
def get_salesmen():
    SALESMAN_LIMIT = 10
    CUSTOMER_LIMIT = 15
    SCAN_LIMIT = 300  # scan more rows to compensate bad data

    db = SessionLocal()

    # 1️⃣ Fetch candidate salesmen (no pagination)
    candidates = db.execute(text("""
        SELECT id, osa_code, name, warehouse_id
        FROM salesman
        WHERE type = 2
        ORDER BY id
        LIMIT :limit
    """), {
        "limit": SCAN_LIMIT
    }).fetchall()

    result = []

    for s in candidates:
        if len(result) == SALESMAN_LIMIT:
            break

        # 2️⃣ Warehouse must exist & have valid position
        warehouse = db.execute(text("""
            SELECT latitude, longitude
            FROM tbl_warehouse
            WHERE id = CAST(:wid AS INTEGER)
        """), {"wid": s.warehouse_id}).fetchone()

        if not warehouse or not is_valid_position(warehouse.latitude, warehouse.longitude):
            continue  # ❌ skip salesman

        # 3️⃣ Customers must exist & ALL must have valid position
        customers = db.execute(text("""
            SELECT c.id, c.osa_code, c.name, c.latitude, c.longitude
            FROM tbl_route r
            JOIN agent_customers c ON c.route_id = r.id
            WHERE r.warehouse_id = CAST(:wid AS INTEGER)
            ORDER BY c.id
            LIMIT :limit
        """), {
            "wid": s.warehouse_id,
            "limit": CUSTOMER_LIMIT
        }).fetchall()

        if not customers:
            continue  # ❌ skip salesman

        clean_customers = []
        invalid_customer = False

        for c in customers:
            if not is_valid_position(c.latitude, c.longitude):
                invalid_customer = True
                break

            clean_customers.append({
                "id": c.id,
                "name": f"{c.osa_code} {c.name}",
                "position": [c.latitude, c.longitude],
                "visited": random.choice([True, False])
            })

        if invalid_customer:
            continue  # ❌ skip salesman

        # ✅ Clean salesman
        result.append({
            "id": s.id,
            "name": f"{s.osa_code} {s.name}",
            "avatar": "https://cdn-icons-png.flaticon.com/512/147/147144.png",
            "position": [warehouse.latitude, warehouse.longitude],
            "customers": clean_customers
        })

    db.close()

    return {
        "returned_salesmen": len(result),
        "data": result
    }
