from fastapi import APIRouter,Query,HTTPException
from sqlalchemy import text
from app.database import engine
from app.dashboard.utils.dashboard_common_helper import parse_csv_ids
from typing import Optional,List


router  = APIRouter()

@router.get("/dashboard-filter")
def dashboard_filter(
    company_ids: Optional[str] = Query(None),
    region_ids: Optional[str] = Query(None),
    area_ids: Optional[str] = Query(None),
    warehouse_ids: Optional[str] = Query(None),
    item_category_ids: Optional[str] = Query(None),
    customer_channel_ids: Optional[str] = Query(None),
    customer_category_ids: Optional[str] = Query(None),
):

    company_ids_list = parse_csv_ids(company_ids)
    region_ids_list = parse_csv_ids(region_ids)
    area_ids_list = parse_csv_ids(area_ids)
    warehouse_ids_list = parse_csv_ids(warehouse_ids)
    item_category_ids_list = parse_csv_ids(item_category_ids)
    customer_channel_ids_list = parse_csv_ids(customer_channel_ids)
    customer_category_ids_list = parse_csv_ids(customer_category_ids)

    out = {}


    try:
        with engine.connect() as conn:

            q = "SELECT id, company_name FROM tbl_company ORDER BY company_name"
            out["companies"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]


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

            q = "SELECT id, category_name FROM item_categories ORDER BY category_name"
            out["item_categories"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

            q = "SELECT id, customer_category_name FROM customer_categories ORDER BY customer_category_name"
            out["customer_categories"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]


          
    except Exception as e:
        print("FILTER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

    return out

