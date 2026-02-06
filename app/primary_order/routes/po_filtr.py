from app.primary_order.utils.po_order_common_helper import parse_csv_ids
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
from typing import Optional
from app.database import engine

router = APIRouter()

@router.get("/po-order-filters")
def get_filters(
    company_ids: Optional[str] = Query(None),
    region_ids: Optional[str] = Query(None),
    area_ids: Optional[str] = Query(None),
    warehouse_ids: Optional[str] = Query(None),
):

    company_ids_list = parse_csv_ids(company_ids)
    region_ids_list = parse_csv_ids(region_ids)
    area_ids_list = parse_csv_ids(area_ids)
    warehouse_ids_list = parse_csv_ids(warehouse_ids)

    out = {}

    try:
        with engine.connect() as conn:

            # ---------------- COMPANIES ----------------
            q = "SELECT id, company_name FROM tbl_company ORDER BY company_name"
            out["companies"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

            # ---------------- REGIONS ----------------
            if company_ids_list:
                q = """
                SELECT id, region_name
                FROM tbl_region
                WHERE company_id IN :company_ids
                ORDER BY region_name
                """
                out["regions"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"company_ids": tuple(company_ids_list)}).fetchall()
                ]
            else:
                q = "SELECT id, region_name FROM tbl_region ORDER BY region_name"
                out["regions"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

            # ---------------- AREAS ----------------
            if region_ids_list:
                q = """
                SELECT id, area_name
                FROM tbl_areas
                WHERE region_id IN :region_ids
                ORDER BY area_name
                """
                out["areas"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"region_ids": tuple(region_ids_list)}).fetchall()
                ]

            elif company_ids_list:
                q = """
                SELECT a.id, a.area_name
                FROM tbl_areas a
                JOIN tbl_region r ON r.id = a.region_id
                WHERE r.company_id IN :company_ids
                ORDER BY a.area_name
                """
                out["areas"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"company_ids": tuple(company_ids_list)}).fetchall()
                ]
            else:
                q = "SELECT id, area_name FROM tbl_areas ORDER BY area_name"
                out["areas"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

            # ---------------- WAREHOUSES ----------------
            if area_ids_list:
                q = """
                SELECT id, warehouse_name
                FROM tbl_warehouse
                WHERE area_id IN :area_ids
                ORDER BY warehouse_name
                """
                out["warehouses"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"area_ids": tuple(area_ids_list)}).fetchall()
                ]

            elif region_ids_list:
                q = """
                SELECT w.id, w.warehouse_name
                FROM tbl_warehouse w
                JOIN tbl_areas a ON a.id = w.area_id
                WHERE a.region_id IN :region_ids
                ORDER BY w.warehouse_name
                """
                out["warehouses"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"region_ids": tuple(region_ids_list)}).fetchall()
                ]

            elif company_ids_list:
                q = """
                SELECT DISTINCT w.id, w.warehouse_name
                FROM tbl_warehouse w
                JOIN tbl_areas a ON a.id = w.area_id
                JOIN tbl_region r ON r.id = a.region_id
                WHERE r.company_id IN :company_ids
                ORDER BY w.warehouse_name
                """
                out["warehouses"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"company_ids": tuple(company_ids_list)}).fetchall()
                ]

            else:
                q = "SELECT id, warehouse_name FROM tbl_warehouse ORDER BY warehouse_name"
                out["warehouses"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

    except Exception as e:
        print("FILTER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

    return out

