from fastapi import APIRouter, HTTPException, Query 
from typing import  Optional
from sqlalchemy import text
from database import engine
from sales_report.utils.filter_helper import parse_csv_ids
from sales_report.utils.filter_helper import normalize_user_field, resolve_effective_ui_vs_user

app = APIRouter()

@app.get("/user_based_filter-for-sales")
def get_filters(
    user_id: int = Query(...),
):

    out = {}

    try:
        with engine.connect() as conn:

            # =====================================================
            # LOAD USER
            # =====================================================
            user = conn.execute(
                text("""
                    SELECT
                        company, region, area, warehouse,
                        route, salesman,
                        outlet_channel, customer_category_id, customer_id,
                        item_category_id, item_id
                    FROM users
                    WHERE id = :uid
                """),
                {"uid": user_id}
            ).fetchone()

            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # =====================================================
            # NORMALIZE USER FIELDS
            # =====================================================
            comp_mode, comp_list = normalize_user_field(user.company)
            reg_mode, reg_list = normalize_user_field(user.region)
            area_mode, area_list = normalize_user_field(user.area)
            wh_mode, wh_list = normalize_user_field(user.warehouse)

            route_mode, route_list = normalize_user_field(user.route)
            salesman_mode, salesman_list = normalize_user_field(user.salesman)

            channel_mode, channel_list = normalize_user_field(user.outlet_channel)
            custcat_mode, custcat_list = normalize_user_field(user.customer_category_id)
            cust_mode, cust_list = normalize_user_field(user.customer_id)

            itemcat_mode, itemcat_list = normalize_user_field(user.item_category_id)
            item_mode, item_list = normalize_user_field(user.item_id)

            # =====================================================
            # BASE LEVEL (STOP AT WAREHOUSE)
            # =====================================================
            if wh_mode == "list":
                base_level, base_ids = "warehouse", wh_list
            elif area_mode == "list":
                base_level, base_ids = "area", area_list
            elif reg_mode == "list":
                base_level, base_ids = "region", reg_list
            elif comp_mode == "list":
                base_level, base_ids = "company", comp_list
            else:
                base_level, base_ids = "all", None

            # =====================================================
            # COMPANIES
            # =====================================================
            if base_level == "company":
                q = "SELECT id, company_name FROM tbl_company WHERE id IN :ids"
                params = {"ids": tuple(base_ids)}
            elif base_level in ("region", "area", "warehouse"):
                q = """
                    SELECT DISTINCT c.id, c.company_name
                    FROM tbl_company c
                    JOIN tbl_region r ON r.company_id = c.id
                    JOIN tbl_areas a ON a.region_id = r.id
                    JOIN tbl_warehouse w ON w.area_id = a.id
                    WHERE w.id IN :ids
                """
                params = {"ids": tuple(base_ids)}
            else:
                q = "SELECT id, company_name FROM tbl_company"
                params = {}

            out["companies"] = conn.execute(text(q), params).mappings().all()

            # =====================================================
            # REGIONS
            # =====================================================
            if base_level == "region":
                q = "SELECT id, region_name FROM tbl_region WHERE id IN :ids"
                params = {"ids": tuple(base_ids)}
            elif base_level in ("area", "warehouse"):
                q = """
                    SELECT DISTINCT r.id, r.region_name
                    FROM tbl_region r
                    JOIN tbl_areas a ON a.region_id = r.id
                    JOIN tbl_warehouse w ON w.area_id = a.id
                    WHERE w.id IN :ids
                """
                params = {"ids": tuple(base_ids)}
            elif base_level == "company":
                q = "SELECT id, region_name FROM tbl_region WHERE company_id IN :ids"
                params = {"ids": tuple(base_ids)}
            else:
                q = "SELECT id, region_name FROM tbl_region"
                params = {}

            out["regions"] = conn.execute(text(q), params).mappings().all()

            # =====================================================
            # AREAS
            # =====================================================
            if base_level == "area":
                q = "SELECT id, area_name, region_id FROM tbl_areas WHERE id IN :ids"
                params = {"ids": tuple(base_ids)}
            elif base_level == "warehouse":
                q = """
                    SELECT a.id, a.area_name, a.region_id
                    FROM tbl_areas a
                    JOIN tbl_warehouse w ON w.area_id = a.id
                    WHERE w.id IN :ids
                """
                params = {"ids": tuple(base_ids)}
            elif base_level == "region":
                q = "SELECT id, area_name, region_id FROM tbl_areas WHERE region_id IN :ids"
                params = {"ids": tuple(base_ids)}
            elif base_level == "company":
                q = """
                    SELECT a.id, a.area_name, a.region_id
                    FROM tbl_areas a
                    JOIN tbl_region r ON r.id = a.region_id
                    WHERE r.company_id IN :ids
                """
                params = {"ids": tuple(base_ids)}
            else:
                q = "SELECT id, area_name, region_id FROM tbl_areas"
                params = {}

            out["areas"] = conn.execute(text(q), params).mappings().all()

            # =====================================================
            # WAREHOUSES (BASE UNIVERSE)
            # =====================================================
            if base_level == "warehouse":
                q = "SELECT id, warehouse_name, area_id FROM tbl_warehouse WHERE id IN :ids"
                params = {"ids": tuple(base_ids)}
            elif base_level == "area":
                q = "SELECT id, warehouse_name, area_id FROM tbl_warehouse WHERE area_id IN :ids"
                params = {"ids": tuple(base_ids)}
            elif base_level == "region":
                q = """
                    SELECT w.id, w.warehouse_name, w.area_id
                    FROM tbl_warehouse w
                    JOIN tbl_areas a ON a.id = w.area_id
                    WHERE a.region_id IN :ids
                """
                params = {"ids": tuple(base_ids)}
            elif base_level == "company":
                q = """
                    SELECT w.id, w.warehouse_name, w.area_id
                    FROM tbl_warehouse w
                    JOIN tbl_areas a ON a.id = w.area_id
                    JOIN tbl_region r ON r.id = a.region_id
                    WHERE r.company_id IN :ids
                """
                params = {"ids": tuple(base_ids)}
            else:
                q = "SELECT id, warehouse_name, area_id FROM tbl_warehouse"
                params = {}

            out["warehouses"] = conn.execute(text(q), params).mappings().all()
            derived_wh_ids = [w["id"] for w in out["warehouses"]]

            # =====================================================
            # CASCADE BELOW WAREHOUSE
            # =====================================================

            # ---------------- ROUTES ----------------
            if route_mode == "list":
                out["routes"] = conn.execute(
                    text("""
                        SELECT id, route_code || ' - ' || route_name AS label
                        FROM tbl_route
                        WHERE id IN :ids
                    """),
                    {"ids": tuple(route_list)}
                ).mappings().all()
            else:
                out["routes"] = conn.execute(
                    text("""
                        SELECT id, route_code || ' - ' || route_name AS label
                        FROM tbl_route
                        WHERE warehouse_id IN :ids
                    """),
                    {"ids": tuple(derived_wh_ids)}
                ).mappings().all()

            derived_route_ids = [r["id"] for r in out["routes"]]

            # ---------------- SALESMEN ----------------
            if salesman_mode == "list":
                out["salesmen"] = conn.execute(
                    text("""
                        SELECT id, osa_code || ' - ' || name AS label
                        FROM salesman
                        WHERE id IN :ids
                    """),
                    {"ids": tuple(salesman_list)}
                ).mappings().all()
            else:
                out["salesmen"] = conn.execute(
                    text("""
                        SELECT id, osa_code || ' - ' || name AS label
                        FROM salesman
                        WHERE route_id IN :ids
                    """),
                    {"ids": tuple(derived_route_ids)}
                ).mappings().all()

            # ---------------- CUSTOMERS ----------------
            if cust_mode == "list":
                q = "SELECT id, osa_code || ' - ' || name AS label FROM agent_customers WHERE id IN :ids"
                params = {"ids": tuple(cust_list)}
            elif custcat_mode == "list":
                q = "SELECT id, osa_code || ' - ' || name AS label FROM agent_customers WHERE category_id IN :ids"
                params = {"ids": tuple(custcat_list)}
            elif channel_mode == "list":
                q = "SELECT id, osa_code || ' - ' || name AS label FROM agent_customers WHERE outlet_channel_id IN :ids"
                params = {"ids": tuple(channel_list)}
            else:
                q = "SELECT id, osa_code || ' - ' || name AS label FROM agent_customers WHERE warehouse IN :ids"
                params = {"ids": tuple(derived_wh_ids)}

            out["customers"] = conn.execute(text(q), params).mappings().all()

            # ---------------- ITEM CATEGORIES ----------------
            if itemcat_mode == "list":
                out["item_categories"] = conn.execute(
                    text("""
                        SELECT id, category_name
                        FROM item_categories
                        WHERE id IN :ids
                    """),
                    {"ids": tuple(itemcat_list)}
                ).mappings().all()
            else:
                out["item_categories"] = conn.execute(
                    text("""
                        SELECT DISTINCT ic.id, ic.category_name
                        FROM invoice_headers ih
                        JOIN invoice_details idl ON idl.header_id = ih.id
                        JOIN items i ON i.id = idl.item_id
                        JOIN item_categories ic ON ic.id = i.category_id
                        WHERE ih.warehouse_id IN :ids
                    """),
                    {"ids": tuple(derived_wh_ids)}
                ).mappings().all()

            derived_itemcat_ids = [c["id"] for c in out["item_categories"]]

            # ---------------- ITEMS ----------------
            if item_mode == "list":
                out["items"] = conn.execute(
                    text("""
                        SELECT id, name, category_id
                        FROM items
                        WHERE id IN :ids
                    """),
                    {"ids": tuple(item_list)}
                ).mappings().all()
            elif itemcat_mode == "list":
                out["items"] = conn.execute(
                    text("""
                        SELECT id, name, category_id
                        FROM items
                        WHERE category_id IN :ids
                    """),
                    {"ids": tuple(itemcat_list)}
                ).mappings().all()
            else:
                out["items"] = conn.execute(
                    text("""
                        SELECT id, name, category_id
                        FROM items
                        WHERE category_id IN :ids
                    """),
                    {"ids": tuple(derived_itemcat_ids)}
                ).mappings().all()

    except Exception as e:
        print("FILTER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

    return out
