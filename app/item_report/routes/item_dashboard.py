from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
from typing import Optional
from datetime import datetime
from app.database import engine
from app.item_report.schemas.item_schema import DashboardRequest
from app.item_report.utils.item_dashboard_helper import choose_granularity, UPC_JOIN
from app.database import engine
from app.item_report.schemas.item_schema import DashboardRequest
from app.item_report.utils.item_dashboard_helper import choose_granularity, UPC_JOIN

from datetime import datetime

router = APIRouter()



# -------------------------------------------------------------------
# DASHBOARD
# -------------------------------------------------------------------

@router.post("/item-report-dashboard")
def dashboard_kpis(payload: DashboardRequest):
    from_date_str = payload.from_date
    to_date_str = payload.to_date

    search_type = payload.search_type
    display_quantity = payload.display_quantity

    company_ids = payload.company_ids
    region_ids = payload.region_ids
    area_ids = payload.area_ids
    warehouse_ids = payload.warehouse_ids
    route_ids = payload.route_ids
    item_category_ids = payload.item_category_ids
    brand_ids = payload.brand_ids
    item_ids = payload.item_ids

    # ---------------- DATE PARSE ----------------
    try:
        from_date = datetime.strptime(from_date_str, "%Y-%m-%d").date()
        to_date = datetime.strptime(to_date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "Dates must be YYYY-MM-DD")


    # ---------------- FILTERS ----------------
    company_ids = company_ids or []
    region_ids = region_ids or []
    area_ids = area_ids or []
    warehouse_ids = warehouse_ids or []
    route_ids = route_ids or []
    item_category_ids = item_category_ids or []
    brand_ids = brand_ids or []
    item_ids = item_ids or []


    show_item_ranking = len(item_ids) > 5

    show_item_ranking = len(item_ids) > 5

    params = {
        "from_date": from_date,
        "to_date": to_date,
        "item_ids": tuple(item_ids)
    }

    filters = [
        "ms.invoice_date BETWEEN :from_date AND :to_date",
    ]#        "ms.item_id IN :item_ids"
    if item_ids:
        filters.append("ms.item_id IN :item_ids")
        params["item_ids"] = tuple(item_ids)

    def add(col, val, name):
        if val:
            filters.append(f"{col} IN :{name}")
            params[name] = tuple(val)

    add("ms.company_id", company_ids, "company_ids")
    add("ms.region_id", region_ids, "region_ids")
    add("ms.area_id", area_ids, "area_ids")
    add("ms.warehouse_id", warehouse_ids, "warehouse_ids")
    add("ms.route_id", route_ids, "route_ids")
    add("ms.item_category_id", item_category_ids, "item_category_ids")
    add("it.brand", brand_ids, "brand_ids")


    def detect_level():
        if item_ids:
            return "item"
        if item_category_ids or brand_ids:
            return "item_group"
        if route_ids:
            return "route"
        if warehouse_ids:
            return "warehouse"
        if area_ids:
            return "area"
        if region_ids:
            return "region"
        if company_ids:
            return "company"
        return "none"


    level = detect_level()

    if level == "item" and not item_ids:
        raise HTTPException(400, "Select at least one item")

    ITEM_LEVELS = {"item", "item_group"}
    COMPANY_LEVELS = {"company", "region", "area", "warehouse", "route"}

    if display_quantity == "without_free_good":
        filters.append("ms.total_amount > 0")

    where_sql = " AND ".join(filters)

    # ---------------- VALUE EXPRESSION ----------------
    if search_type == "quantity":
        sales_expr = """
            SUM(
                CASE
                    WHEN ms.uom IN (1,3)
                        AND NULLIF(iu.upc::numeric, 0) IS NOT NULL
                    THEN ms.total_quantity / iu.upc::numeric
                    ELSE ms.total_quantity
                END
            )
        """

        purchase_expr = """
          SUM(
                CASE
                    WHEN hid.uom_id IN (1,3)
                        AND NULLIF(iu.upc::numeric, 0) IS NOT NULL
                    THEN hid.quantity / iu.upc::numeric
                    ELSE hid.quantity
                END
            )
        """

        return_expr = """
          SUM(
                CASE
                    WHEN rd.uom IN (1,3)
                        AND NULLIF(iu.upc::numeric, 0) IS NOT NULL
                    THEN rd.qty / iu.upc::numeric
                    ELSE rd.qty
                END
            )
        """

    else:
        sales_expr = "SUM(ms.total_amount)"
        purchase_expr = "SUM(hid.item_price)"
        return_expr = "SUM(rd.item_value)"

    granularity, period_sql, order_sql = choose_granularity(
        from_date, to_date
    )

    # ---------------- PERIOD EXPRESSIONS FOR NON-MV TABLES ----------------
    if granularity == "daily":
        purchase_period = "DATE(hih.invoice_date)"
        return_period = "DATE(rh.created_at)"
    elif granularity == "weekly":
        purchase_period = "TO_CHAR(hih.invoice_date, 'IYYY-IW')"
        return_period = "TO_CHAR(rh.created_at, 'IYYY-IW')"
    elif granularity == "monthly":
        purchase_period = "TO_CHAR(hih.invoice_date, 'YYYY-MM')"
        return_period = "TO_CHAR(rh.created_at, 'YYYY-MM')"
    else:
        purchase_period = "TO_CHAR(hih.invoice_date, 'YYYY')"
        return_period = "TO_CHAR(rh.created_at, 'YYYY')"


    with engine.connect() as conn:

        # ================= TOTAL SALES =================
        total_sales = conn.execute(text(f"""
            SELECT ROUND(COALESCE({sales_expr},0)::numeric,3)
            FROM mv_sales_report_fast ms
            LEFT JOIN items it ON it.id = ms.item_id
            LEFT JOIN (
                SELECT item_id, MAX(upc::numeric) AS upc
                FROM item_uoms
                WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
                GROUP BY item_id
            ) iu ON iu.item_id = ms.item_id

            WHERE {where_sql}
        """), params).scalar()

        # ================= TOTAL PURCHASE =================
        total_purchase = conn.execute(text(f"""
            SELECT ROUND(COALESCE({purchase_expr},0)::numeric,3)
            FROM ht_invoice_detail hid
            JOIN ht_invoice_header hih ON hih.id = hid.header_id
            LEFT JOIN item_uoms iu ON iu.item_id = hid.item_id
            WHERE hih.invoice_date BETWEEN :from_date AND :to_date
        """), params).scalar()

        # ================= TOTAL RETURN =================
        total_return = conn.execute(text(f"""
            SELECT ROUND(COALESCE({return_expr},0)::numeric,3)
            FROM ht_return_details rd
            JOIN ht_return_header rh ON rh.id = rd.header_id
            LEFT JOIN item_uoms iu ON iu.item_id = rd.item_id
            WHERE rh.created_at::date BETWEEN :from_date AND :to_date
        """), params).scalar()

        # ================= TOTAL ITEMS =================
        if level == "item":
            total_items = len(item_ids)
        else:
            total_items = conn.execute(text(f"""
                SELECT COUNT(DISTINCT ms.item_id)
                FROM mv_sales_report_fast ms
                WHERE {where_sql}
            """), params).scalar()





        # ================= SALES VALUE EXPRESSION =================

        sales_value_expr = """
        CASE
            WHEN ms.uom IN (1,3)
                AND NULLIF(iu.upc, 0) IS NOT NULL
            THEN ms.total_quantity / iu.upc
            ELSE ms.total_quantity
        END
        """


        # ================= REGION SALES =================
        region_sales = conn.execute(text(f"""
            SELECT
                ms.region_id,
                ms.region_name,
                ROUND(
                    COALESCE(SUM({sales_value_expr}), 0)::numeric,
                    3
                ) AS total_sales
            FROM mv_sales_report_fast ms
            {UPC_JOIN}
            LEFT JOIN items it ON it.id = ms.item_id
            WHERE {where_sql}
            GROUP BY ms.region_id, ms.region_name
            ORDER BY total_sales DESC
        """), params).mappings().all()

        # ================= AREA SALES =================
        area_sales = conn.execute(text(f"""
            SELECT
                ms.area_id,
                ms.area_name,
                ROUND(
                    COALESCE(SUM({sales_value_expr}), 0)::numeric,
                    3
                ) AS total_sales
            FROM mv_sales_report_fast ms
            {UPC_JOIN}
            LEFT JOIN items it ON it.id = ms.item_id
            WHERE {where_sql}
            GROUP BY ms.area_id, ms.area_name
            ORDER BY total_sales DESC
        """), params).mappings().all()



        # ================= SALES TREND =================
        sales_trend = conn.execute(text(f"""
            SELECT
                {period_sql} AS period,
                ROUND(
                    COALESCE(SUM({sales_value_expr}), 0)::numeric,
                    3
                ) AS total_sales
            FROM mv_sales_report_fast ms
            {UPC_JOIN}
            LEFT JOIN items it ON it.id = ms.item_id
            WHERE {where_sql}
            GROUP BY period, {order_sql}
            ORDER BY {order_sql}
        """), params).mappings().all()


        # ================= PURCHASE TREND =================
        purchase_trend = conn.execute(text(f"""
            SELECT
                {purchase_period} AS period,
                ROUND(COALESCE({purchase_expr},0)::numeric,3) AS total_purchase
            FROM ht_invoice_detail hid
            JOIN ht_invoice_header hih ON hih.id = hid.header_id
            LEFT JOIN item_uoms iu ON iu.item_id = hid.item_id
            WHERE hih.invoice_date BETWEEN :from_date AND :to_date
            GROUP BY period
            ORDER BY period
        """), params).mappings().all()


        # ================= RETURN TREND =================
        return_trend = conn.execute(text(f"""
            SELECT
                {return_period} AS period,
                ROUND(COALESCE({return_expr},0)::numeric,3) AS total_return
            FROM ht_return_details rd
            JOIN ht_return_header rh ON rh.id = rd.header_id
            LEFT JOIN item_uoms iu ON iu.item_id = rd.item_id
            WHERE rh.created_at::date BETWEEN :from_date AND :to_date
            GROUP BY period
            ORDER BY period
        """), params).mappings().all()


       
        top_5_sales = least_5_sales = top_5_purchase = least_5_purchase = None

        if level in ITEM_LEVELS and show_item_ranking:

            top_5_sales = conn.execute(text(f"""
                SELECT ms.item_id, ms.item_name,
                    ROUND(COALESCE(SUM({sales_value_expr}),0)::numeric,3) AS value
                FROM mv_sales_report_fast ms
                {UPC_JOIN}
                WHERE {where_sql}
                GROUP BY ms.item_id, ms.item_name
                ORDER BY value DESC
                LIMIT 5
            """), params).mappings().all()

            least_5_sales = conn.execute(text(f"""
                SELECT ms.item_id, ms.item_name,
                    ROUND(COALESCE(SUM({sales_value_expr}),0)::numeric,3) AS value
                FROM mv_sales_report_fast ms
                {UPC_JOIN}
                WHERE {where_sql}
                GROUP BY ms.item_id, ms.item_name
                ORDER BY value ASC
                LIMIT 5
            """), params).mappings().all()

            top_5_purchase = conn.execute(text(f"""
                SELECT hid.item_id, i.name AS item_name,
                    ROUND(COALESCE(SUM(hid.quantity),0)::numeric,3) AS value
                FROM ht_invoice_detail hid
                JOIN ht_invoice_header hih ON hih.id = hid.header_id
                JOIN items i ON i.id = hid.item_id
                WHERE hih.invoice_date BETWEEN :from_date AND :to_date
                GROUP BY hid.item_id, i.name
                ORDER BY value DESC
                LIMIT 5
            """), params).mappings().all()

            least_5_purchase = conn.execute(text(f"""
                SELECT hid.item_id, i.name AS item_name,
                    ROUND(COALESCE(SUM(hid.quantity),0)::numeric,3) AS value
                FROM ht_invoice_detail hid
                JOIN ht_invoice_header hih ON hih.id = hid.header_id
                JOIN items i ON i.id = hid.item_id
                WHERE hih.invoice_date BETWEEN :from_date AND :to_date
                GROUP BY hid.item_id, i.name
                ORDER BY value ASC
                LIMIT 5
            """), params).mappings().all()

        top_10_sales = least_10_sales = top_10_purchase = least_10_purchase = None

        if level in COMPANY_LEVELS:

            top_10_sales = conn.execute(text(f"""
                SELECT ms.item_id, ms.item_name,
                    ROUND(COALESCE(SUM({sales_value_expr}),0)::numeric,3) AS value
                FROM mv_sales_report_fast ms
                {UPC_JOIN}
                WHERE {where_sql}
                GROUP BY ms.item_id, ms.item_name
                ORDER BY value DESC
                LIMIT 10
            """), params).mappings().all()

            least_10_sales = conn.execute(text(f"""
                SELECT ms.item_id, ms.item_name,
                    ROUND(COALESCE(SUM({sales_value_expr}),0)::numeric,3) AS value
                FROM mv_sales_report_fast ms
                {UPC_JOIN}
                WHERE {where_sql}
                GROUP BY ms.item_id, ms.item_name
                ORDER BY value ASC
                LIMIT 10
            """), params).mappings().all()

            top_10_purchase = conn.execute(text(f"""
                SELECT hid.item_id, i.name AS item_name,
                    ROUND(COALESCE(SUM(hid.quantity),0)::numeric,3) AS value
                FROM ht_invoice_detail hid
                JOIN ht_invoice_header hih ON hih.id = hid.header_id
                JOIN items i ON i.id = hid.item_id
                WHERE hih.invoice_date BETWEEN :from_date AND :to_date
                GROUP BY hid.item_id, i.name
                ORDER BY value DESC
                LIMIT 10
            """), params).mappings().all()

            least_10_purchase = conn.execute(text(f"""
                SELECT hid.item_id, i.name AS item_name,
                    ROUND(COALESCE(SUM(hid.quantity),0)::numeric,3) AS value
                FROM ht_invoice_detail hid
                JOIN ht_invoice_header hih ON hih.id = hid.header_id
                JOIN items i ON i.id = hid.item_id
                WHERE hih.invoice_date BETWEEN :from_date AND :to_date
                GROUP BY hid.item_id, i.name
                ORDER BY value ASC
                LIMIT 10
            """), params).mappings().all()
        response = {
            "level": level,
            "kpis": {
                "total_items": total_items,
                "total_sales": total_sales,
                "total_purchase": total_purchase,
                "total_return": total_return,
            },
            "region_wise_item_performance": region_sales,
            "area_wise_item_performance": area_sales,
            "trend": {
                "granularity": granularity,
                "sales": sales_trend,
                "purchase": purchase_trend,
                "return": return_trend,
            }
        }

        if level in ITEM_LEVELS and show_item_ranking:
            response["item_ranking"] = {
                "top_5_sales": top_5_sales,
                "least_5_sales": least_5_sales,
                "top_5_purchase": top_5_purchase,
                "least_5_purchase": least_5_purchase,
            }

        if level in COMPANY_LEVELS:
            response["item_ranking"] = {
                "top_10_sales": top_10_sales,
                "least_10_sales": least_10_sales,
                "top_10_purchase": top_10_purchase,
                "least_10_purchase": least_10_purchase,
            }


    return response

