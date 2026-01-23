from fastapi import APIRouter
from sqlalchemy import text
from database import engine
from customer_report.schemas.customer_sales_schema import FilterSelection
from customer_report.utils.common_helper import build_query_parts, validate_mandatory
from customer_report.utils.customer_sales_dashboard_helper import choose_granularity, customer_level_filter
import time

app = APIRouter()


@app.post("/customer-sales-dashboard")
def customer_sales_dashboard(filters: FilterSelection):
    """
    Customer Sales Dashboard

    KPIs:
    - Total Sales
    - Total Customers
    - Active Sales Customers (item_total > 0)
    - Inactive Sales Customers (item_total <= 0)

    Charts:
    - Sales Trend
    - Channel-wise Sales (%)
    - Customer Category-wise Sales (%)
    - Top 10 Items
    - Top 10 Customers
    - Top 10 Channels
    - Top 10 Customer Categories
    """

    # --------------------------------------------------
    # Validate & setup
    # --------------------------------------------------
    validate_mandatory(filters)

    granularity, period_label_sql, order_by_sql = choose_granularity(
        filters.from_date, filters.to_date
    )

    joins, where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)
    join_sql = "\n".join(joins)

    # Determine hierarchy level
    if filters.route_ids:
        level = "route"
    elif filters.warehouse_ids:
        level = "warehouse"
    elif filters.area_ids:
        level = "area"
    elif filters.region_ids:
        level = "region"
    else:
        level = "company"


    if filters.search_type.lower() == "quantity":
        value_expr = """
            ROUND(
                CAST(
                    SUM(
                        CASE
                            WHEN id.uom IN (1, 3)
                                AND iu.upc ~ '^[0-9]+(\\.[0-9]+)?$'
                                AND CAST(iu.upc AS NUMERIC) > 0
                            THEN id.quantity / CAST(iu.upc AS NUMERIC)
                            ELSE id.quantity
                        END
                    ) AS NUMERIC
                ),
                3
            )
        """
    else:
        value_expr = "ROUND(CAST(SUM(id.item_total) AS NUMERIC), 3)"


    out = {
        "level": level,
        "granularity": granularity,
        "kpis": {},
        "charts": {},
    }

    # ==================================================
    # KPIs
    # ==================================================
    start = time.time()
    with engine.connect() as conn:

        # --------------------------------------------------
        # 1️⃣ TOTAL SALES
        # --------------------------------------------------
        sql = f"""
            SELECT COALESCE({value_expr}, 0) AS total_sales
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                    SELECT
                        item_id,
                        MAX(upc) AS upc
                    FROM item_uoms
                    WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id

            {join_sql}
            WHERE {where_sql}
            """
        out["kpis"]["total_sales"] = conn.execute(text(sql), params).scalar()

       
        customer_scope = customer_level_filter(level)

        sql = f"""
        WITH eligible_customers AS (
            SELECT ac.id
            FROM agent_customers ac
            WHERE
                ac.created_at::date <= :to_date
                AND {customer_scope}
        ),
        sales_by_customer AS (
            SELECT DISTINCT ih.customer_id
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                    SELECT
                        item_id,
                        MAX(upc) AS upc
                    FROM item_uoms
                    WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id


            {join_sql}
            WHERE {where_sql}
        )
        SELECT
            COUNT(ec.id) AS total_customers,
            COUNT(s.customer_id) AS active_sales_customers,
            COUNT(ec.id) - COUNT(s.customer_id) AS inactive_sales_customers
        FROM eligible_customers ec
        LEFT JOIN sales_by_customer s
            ON s.customer_id = ec.id
        """


        row = conn.execute(text(sql), params).mappings().one()

        out["kpis"]["total_customers"] = row["total_customers"]
        out["kpis"]["active_sales_customers"] = row["active_sales_customers"]
        out["kpis"]["inactive_sales_customers"] = row["inactive_sales_customers"]
        # ==================================================
        # CHARTS
        # ==================================================

        # 5️⃣ Sales Trend
        sql = f"""
            SELECT
                {period_label_sql} AS period,
                {value_expr} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                    SELECT
                        item_id,
                        MAX(upc) AS upc
                    FROM item_uoms
                    WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id


            {join_sql}
            WHERE {where_sql}
            GROUP BY period, {order_by_sql}
            ORDER BY {order_by_sql}
        """
        out["charts"]["sales_trend"] = [
            dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
        ]

        # 6️⃣ Channel-wise Sales (%)
        sql = f"""
            SELECT
                oc.outlet_channel_code || '-' || oc.outlet_channel AS channel_name,
                {value_expr} AS value,
                ROUND(
                    ({value_expr} /
                     NULLIF(SUM({value_expr}) OVER (),0))::numeric * 100,
                    2
                ) AS percentage
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                    SELECT
                        item_id,
                        MAX(upc) AS upc
                    FROM item_uoms
                    WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id


            JOIN agent_customers cst ON cst.id = ih.customer_id
            JOIN outlet_channel oc ON oc.id = cst.outlet_channel_id
            {join_sql}
            WHERE {where_sql}
            GROUP BY oc.outlet_channel, oc.outlet_channel_code
            ORDER BY value DESC
        """
        out["charts"]["channel_sales"] = [
            dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
        ]

        # 7️⃣ Customer Category-wise Sales (%)
        sql = f"""
            SELECT
               cc.customer_category_code || '-' ||  cc.customer_category_name AS customer_category_name,
                {value_expr} AS value,
                ROUND(
                    ({value_expr}/
                     NULLIF(SUM({value_expr}) OVER (),0))::numeric  * 100,
                    2
                ) AS percentage
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                    SELECT
                        item_id,
                        MAX(upc) AS upc
                    FROM item_uoms
                    WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id

            JOIN agent_customers cst ON cst.id = ih.customer_id
            JOIN customer_categories cc ON cc.id = cst.category_id
            {join_sql}
            WHERE {where_sql}
            GROUP BY cc.customer_category_name, cc.customer_category_code
            ORDER BY value DESC
        """
        out["charts"]["customer_category_sales"] = [
            dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
        ]

        # 8️⃣ Top 10 Items
        sql = f"""
            SELECT
                it.name AS name,
                {value_expr} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                    SELECT
                        item_id,
                        MAX(upc) AS upc
                    FROM item_uoms
                    WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id

            JOIN items it ON it.id = id.item_id
            {join_sql}
            WHERE {where_sql}
            GROUP BY it.name
            ORDER BY value DESC
            LIMIT 10
        """
        out["charts"]["top_items"] = [
            dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
        ]

        # 9️⃣ Top 10 Customers
        sql = f"""
            SELECT
                cst.osa_code || ' - ' || cst.name AS Customers_name,
                {value_expr} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                        SELECT
                            item_id,
                            MAX(upc) AS upc
                        FROM item_uoms
                        WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
                        GROUP BY item_id
                    ) iu ON iu.item_id = id.item_id

            JOIN agent_customers cst ON cst.id = ih.customer_id
            {join_sql}
            WHERE {where_sql}
            GROUP BY cst.id, cst.osa_code, cst.name
            ORDER BY value DESC
            LIMIT 10
        """
        out["charts"]["top_customers"] = [
            dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
        ]

        # 🔟 Top 10 Channels
        sql = f"""
            SELECT
               oc.outlet_channel_code || '-' || oc.outlet_channel AS channel_name,
                {value_expr} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                    SELECT
                        item_id,
                        MAX(upc) AS upc
                    FROM item_uoms
                    WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id

            JOIN agent_customers cst ON cst.id = ih.customer_id
            JOIN outlet_channel oc ON oc.id = cst.outlet_channel_id
            {join_sql}
            WHERE {where_sql}
            GROUP BY oc.outlet_channel, oc.outlet_channel_code
            ORDER BY value DESC
            LIMIT 10
        """
        out["charts"]["top_channels"] = [
            dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
        ]

        # 1️⃣ Top 10 Customer Categories
        sql = f"""
            SELECT
                cc.customer_category_code || '-' ||  cc.customer_category_name AS customer_category_name,
                {value_expr} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                        SELECT
                            item_id,
                            MAX(upc) AS upc
                        FROM item_uoms
                        WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
                        GROUP BY item_id
                    ) iu ON iu.item_id = id.item_id

            JOIN agent_customers cst ON cst.id = ih.customer_id
            JOIN customer_categories cc ON cc.id = cst.category_id
            {join_sql}
            WHERE {where_sql}
            GROUP BY cc.customer_category_name,cc.customer_category_code
            ORDER BY value DESC
            LIMIT 10
        """
        out["charts"]["top_customer_categories"] = [
            dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
        ]
    end = time.time()
    print(f"Dashboard query execution time: {end - start}")
    return out
