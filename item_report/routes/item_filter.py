from item_report.utils.common_helper import parse_csv_ids
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
from typing import Optional
from database import engine




app = APIRouter()


@app.get("/item-filters")
def get_filters(
    company_ids: Optional[str] = Query(None),
    region_ids: Optional[str] = Query(None),
    area_ids: Optional[str] = Query(None),
    warehouse_ids: Optional[str] = Query(None),
    route_ids : Optional[str] = Query(None),
    item_category_ids: Optional[str] = Query(None),
    brand_ids: Optional[str] = Query(None),
    item_ids: Optional[str] = Query(None),

):

    # Convert comma-separated IDs → List[int]
    company_ids_list = parse_csv_ids(company_ids)
    region_ids_list = parse_csv_ids(region_ids)
    area_ids_list = parse_csv_ids(area_ids)
    warehouse_ids_list = parse_csv_ids(warehouse_ids)
    route_ids_list = parse_csv_ids(route_ids)
    item_category_ids_list = parse_csv_ids(item_category_ids)
    brand_ids_list = parse_csv_ids(brand_ids)
    item_ids_list = parse_csv_ids(item_ids)

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

    


            if warehouse_ids_list:
                q = """ 
                    SELECT id, route_code || ' - ' || route_name AS label
                    FROM tbl_route
                    WHERE warehouse_id IN :warehouse_ids
                    ORDER BY route_code
                    """
                out["routes"] = [
                        dict(r._mapping)
                        for r in conn.execute(text(q), {"warehouse_ids": tuple(warehouse_ids_list)}).fetchall()
                ]
            
            elif area_ids_list:
                q = """
                    SELECT tr.id, tr.route_code || ' - ' || tr.route_name AS label
                    FROM tbl_route AS tr
                    JOIN tbl_warehouse w ON w.id = tr.warehouse_id
                    WHERE w.area_id IN :area_ids
                    ORDER BY tr.route_code
                    """
                out["routes"] = [
                        dict(r._mapping)
                        for r in conn.execute(text(q), {"area_ids" : tuple(area_ids_list)}).fetchall()
                ]
            
            elif region_ids_list:
                q = """
                    SELECT tr.id, tr.route_code || ' - ' || tr.route_name AS label
                    FROM tbl_route AS tr
                    JOIN tbl_warehouse w ON w.id = tr.warehouse_id
                    WHERE w.region_id IN :region_ids
                    ORDER BY tr.route_code
                    """
                out["routes"] = [
                        dict(r._mapping)
                        for r in conn.execute(text(q), {"region_ids" : tuple(region_ids_list)}).fetchall()
                ]

            elif company_ids_list:
                q = """
                    SELECT DISTINCT tr.id, tr.route_code || ' - ' || tr.route_name AS label
                    FROM tbl_route AS tr
                    JOIN tbl_warehouse w ON w.id = tr.warehouse_id
                    JOIN tbl_region r ON r.id = w.region_id
                    WHERE r.company_id IN :company_ids
                    ORDER BY label
                    """
                out["routes"] = [
                        dict(r._mapping)
                        for r in conn.execute(text(q), {"company_ids" : tuple(company_ids_list)}).fetchall()
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
            # ------------------------------------------
            # ITEM CATEGORIES
            # ------------------------------------------
            if item_category_ids_list:
                q = """
                    SELECT id, category_name
                    FROM item_categories
                    WHERE id IN :category_ids
                    ORDER BY category_name
                """
                out["item_categories"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q), {"category_ids": tuple(item_category_ids_list)}
                    ).fetchall()
                ]
            else:
                q = """
                    SELECT id, category_name
                    FROM item_categories
                    ORDER BY category_name
                """
                out["item_categories"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q)).fetchall()
                ]
            # ------------------------------------------
            # ITEM BRANDS
            # ------------------------------------------
            if brand_ids_list:
                q = """
                    SELECT id, osa_code || ' ' || name AS brand_name
                    FROM tbl_brands
                    WHERE id IN :brand_ids
                    ORDER BY brand_name
                """
                out["item_brands"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q), {"brand_ids": tuple(brand_ids_list)}
                    ).fetchall()
                ]
            else:
                q = """
                    SELECT id, osa_code || ' ' || name AS brand_name
                    FROM tbl_brands
                    ORDER BY brand_name
                """
                out["item_brands"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q)).fetchall()
                ]
            # ------------------------------------------
            # ITEMS
            # ------------------------------------------
            if item_ids_list:
                q = """
                    SELECT id, code || '-' || name AS item_name
                    FROM items
                    WHERE id IN :item_ids
                    ORDER BY item_name
                """
                out["items"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q), {"item_ids": tuple(item_ids_list)}
                    ).fetchall()
                ]

            elif item_category_ids_list:
                q = """
                    SELECT id, code || '-' || name AS item_name
                    FROM items
                    WHERE category_id IN :category_ids
                    ORDER BY item_name
                """
                out["items"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q), {"category_ids": tuple(item_category_ids_list)}
                    ).fetchall()
                ]

            elif brand_ids_list:
                q = """
                    SELECT id, code || '-' || name AS item_name
                    FROM items
                    WHERE brand IN :brand_ids
                    ORDER BY item_name
                """
                out["items"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q), {"brand_ids": tuple(brand_ids_list)}
                    ).fetchall()
                ]

            else:
                q = """
                    SELECT id, code || '-' || name AS item_name
                    FROM items
                    ORDER BY item_name
                """
                out["items"] = [
                    dict(r._mapping)
                    for r in conn.execute(text(q)).fetchall()
                ]



    except Exception as e:
        print("FILTER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

    return out
