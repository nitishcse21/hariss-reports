from app.sales_report.utils.sales_common_helper import parse_csv_ids
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
from typing import Optional
from app.database import engine

router = APIRouter()

@router.get("/sales-report-filters")
def get_filters(
    company_ids: Optional[str] = Query(None),
    region_ids: Optional[str] = Query(None),
    area_ids: Optional[str] = Query(None),
    warehouse_ids: Optional[str] = Query(None),
    search_by: Optional[str] = Query(None),
    item_category_ids: Optional[str] = Query(None),
    customer_channel_ids: Optional[str] = Query(None),
    customer_category_ids: Optional[str] = Query(None),
):

    # Convert comma-separated IDs → List[int]
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

          
            # REGIONS
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

            elif region_ids_list:
                q = """
                SELECT id, region_name
                FROM tbl_region
                WHERE id IN :region_ids
                ORDER BY region_name
                """
                out["regions"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"region_ids": tuple(region_ids_list)}).fetchall()
                ]

            else:
                q = "SELECT id, region_name FROM tbl_region ORDER BY region_name"
                out["regions"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

          
            # AREAS
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

            # ------------------------------------------
            # WAREHOUSES
            # ------------------------------------------
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

            # ------------------------------------------------------
            # DERIVED WAREHOUSE IDS
            # ------------------------------------------------------
            derived_wh_ids = warehouse_ids_list

            if not derived_wh_ids:
                if area_ids_list:
                    q = "SELECT id FROM tbl_warehouse WHERE area_id IN :area_ids"
                    derived_wh_ids = [
                        r._mapping["id"]
                        for r in conn.execute(text(q), {"area_ids": tuple(area_ids_list)}).fetchall()
                    ]

                elif region_ids_list:
                    q = """
                    SELECT w.id
                    FROM tbl_warehouse w
                    JOIN tbl_areas a ON a.id = w.area_id
                    WHERE a.region_id IN :region_ids
                    """
                    derived_wh_ids = [
                        r._mapping["id"]
                        for r in conn.execute(text(q), {"region_ids": tuple(region_ids_list)}).fetchall()
                    ]

                elif company_ids_list:
                    q = """
                    SELECT DISTINCT w.id
                    FROM tbl_warehouse w
                    JOIN tbl_areas a ON a.id = w.area_id
                    JOIN tbl_region r ON r.id = a.region_id
                    WHERE r.company_id IN :company_ids
                    """
                    derived_wh_ids = [
                        r._mapping["id"]
                        for r in conn.execute(text(q), {"company_ids": tuple(company_ids_list)}).fetchall()
                    ]

            # ------------------------------------------------------
            # ROUTES & SALESMEN
            # ------------------------------------------------------
            if search_by and search_by.lower() == "route":

                if derived_wh_ids:
                    q = """
                    SELECT id, route_code || ' - ' || route_name AS label
                    FROM tbl_route
                    WHERE warehouse_id IN :warehouse_ids
                    ORDER BY route_code
                    """
                    out["routes"] = [
                        dict(r._mapping)
                        for r in conn.execute(text(q), {"warehouse_ids": tuple(derived_wh_ids)}).fetchall()
                    ]
                else:
                    q = """
                    SELECT id, route_code || ' - ' || route_name AS label
                    FROM tbl_route
                    ORDER BY route_code
                    """
                    out["routes"] = [
                        dict(r._mapping)
                        for r in conn.execute(text(q)).fetchall()
                    ]

                out["salesmen"] = []
                

            elif search_by and search_by.lower() == "salesman":

                if derived_wh_ids:
                    wh_text_ids = [str(wid) for wid in derived_wh_ids]

                    q = """
                    SELECT 
                        s.id,
                        s.osa_code || ' - ' || s.name AS label
                    FROM salesman s
                    WHERE EXISTS (
                        SELECT 1 
                        FROM unnest(string_to_array(s.warehouse_id, ',')) AS wid
                        WHERE wid = ANY(:warehouse_ids)
                    )
                    ORDER BY s.name
                    """

                    out["salesmen"] = [
                        dict(r._mapping)
                        for r in conn.execute(text(q), {"warehouse_ids": wh_text_ids}).fetchall()
                    ]
                else:
                    q = """
                    SELECT 
                        s.id,
                        s.osa_code || ' - ' || s.name AS label
                    FROM salesman s
                    ORDER BY s.name
                    """
                    out["salesmen"] = [
                        dict(r._mapping)
                        for r in conn.execute(text(q)).fetchall()
                    ]

                out["routes"] = []



            else:
                if derived_wh_ids:
                    # ROUTES
                    q_route = """
                    SELECT id, route_code || ' - ' || route_name AS label
                    FROM tbl_route
                    WHERE warehouse_id IN :warehouse_ids
                    ORDER BY route_code
                    """
                    out["routes"] = [
                        dict(r._mapping)
                        for r in conn.execute(text(q_route), {"warehouse_ids": tuple(derived_wh_ids)}).fetchall()
                    ]

                    # SALESMEN
                    wh_text_ids = [str(wid) for wid in derived_wh_ids]
                    q_sales = """
                    SELECT 
                        s.id,
                        s.osa_code || ' - ' || s.name AS label
                    FROM salesman s
                    WHERE EXISTS (
                        SELECT 1 
                        FROM unnest(string_to_array(s.warehouse_id, ',')) AS wid
                        WHERE wid = ANY(:warehouse_ids_text)
                    )
                    ORDER BY s.name
                    """

                    out["salesmen"] = [
                        dict(r._mapping)
                        for r in conn.execute(
                            text(q_sales), {"warehouse_ids_text": wh_text_ids}
                        ).fetchall()
                    ]
                else:
                    out["routes"], out["salesmen"] = [], []


            # ------------------------------------------
            # ITEM CATEGORIES & ITEMS
            # ------------------------------------------
            q = "SELECT id, category_name FROM item_categories ORDER BY category_name"
            out["item_categories"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

            out["items"] = []
            if item_category_ids_list:
                q = """
                SELECT id, name
                FROM items
                WHERE category_id IN :item_category_ids
                ORDER BY name
                """
                out["items"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"item_category_ids": tuple(item_category_ids_list)}).fetchall()
                ]
            else:
                q = "SELECT id, name FROM items ORDER BY name"
                out["items"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]
            # ------------------------------------------
            # CUSTOMER CHANNEL / CATEGORY / CUSTOMERS
            # ------------------------------------------
            # Always load channel categories
            q = "SELECT id, outlet_channel FROM outlet_channel ORDER BY outlet_channel"
            out["channel_categories"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

            # If channel filter applied → filter categories
            if customer_channel_ids_list:
                q = """
                SELECT id, customer_category_name
                FROM customer_categories
                WHERE outlet_channel_id IN :ids
                ORDER BY customer_category_name
                """
                out["customer_categories"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"ids": tuple(customer_channel_ids_list)}).fetchall()
                ]
            else:
                q = "SELECT id, customer_category_name FROM customer_categories ORDER BY customer_category_name"
                out["customer_categories"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

            # Customer filter logic
            if customer_channel_ids_list and customer_category_ids_list:
                q = """
                SELECT id, osa_code || ' - ' || name AS label
                FROM agent_customers
                WHERE outlet_channel_id IN :channels
                AND category_id IN :categories
                ORDER BY name
                """
                out["customers"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q),
                        {"channels": tuple(customer_channel_ids_list), "categories": tuple(customer_category_ids_list)},
                    ).fetchall()
                ]

            elif customer_channel_ids_list:
                q = """
                SELECT id, osa_code || ' - ' || name AS label
                FROM agent_customers
                WHERE outlet_channel_id IN :channels
                ORDER BY name
                """
                out["customers"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"channels": tuple(customer_channel_ids_list)}).fetchall()
                ]

            elif customer_category_ids_list:
                q = """
                SELECT id, osa_code || ' - ' || name AS label
                FROM agent_customers
                WHERE category_id IN :categories
                ORDER BY name
                """
                out["customers"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"categories": tuple(customer_category_ids_list)}).fetchall()
                ]

            else:
                out["customers"] = []

    except Exception as e:
        print("FILTER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

    return out




@router.get("/filter-customer")
def filter_customer(
    customer_channel_ids: Optional[str] = Query(None),
    customer_category_ids: Optional[str] = Query(None),
):
    
    customer_channel_ids_list = parse_csv_ids(customer_channel_ids)
    customer_category_ids_list = parse_csv_ids(customer_category_ids)

    out = {}

    try:
        with engine.connect() as conn:
            q = "SELECT id, outlet_channel FROM outlet_channel ORDER BY outlet_channel"
            out["channel_categories"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

            # If channel filter applied → filter categories
            if customer_channel_ids_list:
                q = """
                SELECT id, customer_category_name
                FROM customer_categories
                WHERE outlet_channel_id IN :ids
                ORDER BY customer_category_name
                """
                out["customer_categories"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"ids": tuple(customer_channel_ids_list)}).fetchall()
                ]
            else:
                q = "SELECT id, customer_category_name FROM customer_categories ORDER BY customer_category_name"
                out["customer_categories"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]

            # Customer filter logic
            if customer_channel_ids_list and customer_category_ids_list:
                q = """
                SELECT id, osa_code || ' - ' || name AS label
                FROM agent_customers
                WHERE outlet_channel_id IN :channels
                AND category_id IN :categories
                ORDER BY name
                """
                out["customers"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q),
                        {"channels": tuple(customer_channel_ids_list), "categories": tuple(customer_category_ids_list)},
                    ).fetchall()
                ]

            elif customer_channel_ids_list:
                q = """
                SELECT id, osa_code || ' - ' || name AS label
                FROM agent_customers
                WHERE outlet_channel_id IN :channels
                ORDER BY name
                """
                out["customers"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"channels": tuple(customer_channel_ids_list)}).fetchall()
                ]

            elif customer_category_ids_list:
                q = """
                SELECT id, osa_code || ' - ' || name AS label
                FROM agent_customers
                WHERE category_id IN :categories
                ORDER BY name
                """
                out["customers"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q), {"categories": tuple(customer_category_ids_list)}).fetchall()
                ]

            else:
                out["customers"] = []

    except Exception as e:
        print("FILTER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

    return out