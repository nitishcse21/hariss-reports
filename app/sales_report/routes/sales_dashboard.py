from app.sales_report.schemas.sales_schema import FilterSelection
from fastapi import APIRouter
from sqlalchemy import text
from app.sales_report.utils.sales_common_helper import build_query_parts, validate_mandatory, quantity_expr_sql
from app.database import engine
from app.sales_report.utils.dashboard_helper import ensure_warehouse_join, choose_granularity, get_top_tables

router = APIRouter()

@router.post("/sales-dashboard")
def get_dashboard(filters: FilterSelection):

    validate_mandatory(filters)
    granularity, period_label_sql, order_by_sql = choose_granularity(
        filters.from_date, filters.to_date
    )
    joins, where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)
    join_sql_base = "\n".join(joins)

    if filters.warehouse_ids:
        level = "warehouse"
    elif filters.area_ids:
        level = "area"
    elif filters.region_ids:
        level = "region"
    else:
        level = "company"

    out = {
        "level": level,
        "granularity": granularity,
        "charts": {},
        "tables": {},
    }
    qauntity = quantity_expr_sql()
    value_expr = (
        qauntity
        if filters.search_type.lower() == "quantity"
        else "SUM(id.item_total)"
    )

    # ------------------------------------------------------------------
    # COMPANY LABEL DASHBOARD
    # ------------------------------------------------------------------

    if level == "company":

        # 1. Company wise sales
        sql = f"""
            SELECT
                c.company_name,
                {value_expr} as value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN tbl_company c ON c.id = ih.company_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {join_sql_base}
            WHERE {where_sql}
            GROUP BY c.company_name
            ORDER BY value DESC
        """
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        out["charts"]["company_sales"] = [dict(r._mapping) for r in rows]

        # 2. Region wise sales pie chart
        sql = f"""
            SELECT
                r.region_name,
                {value_expr} as value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN tbl_warehouse w ON w.id = ih.warehouse_id
            JOIN tbl_region r ON r.id = w.region_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {join_sql_base}
            WHERE {where_sql}
            GROUP BY r.region_name
            ORDER BY value DESC
        """
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        out["charts"]["region_sales"] = [dict(r._mapping) for r in rows]

        # 3. Area wise sales bar chart
        sql = f"""
            SELECT
                a.area_name,
                {value_expr} as value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN tbl_warehouse w ON w.id = ih.warehouse_id
            JOIN tbl_areas a ON a.id = w.area_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {join_sql_base}
            WHERE {where_sql}
            GROUP BY a.area_name
            ORDER BY value DESC
        """
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        out["charts"]["area_sales"] = [dict(r._mapping) for r in rows]

        # 4. Company wise sales trend line chart (by day/week/month)
        sql = f"""
            SELECT
                {period_label_sql} AS period,
                c.company_name,
                {value_expr} as value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN tbl_company c ON c.id = ih.company_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {join_sql_base}
            WHERE {where_sql}
            GROUP BY period, c.company_name,{order_by_sql}
            ORDER BY {order_by_sql}, c.company_name
        """
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        out["charts"]["company_sales_trend"] = [dict(r._mapping) for r in rows]

        joins_ws = joins.copy()
        ensure_warehouse_join(joins_ws)
        join_sql_ws = "\n".join(joins_ws)

        with engine.connect() as conn:

            top_tables = get_top_tables(
                conn,
                value_expr=value_expr,
                where_sql=where_sql,
                params=params,
                join_sql_base=join_sql_base,
                join_sql_ws=join_sql_ws,
                limit=10,
            )

        out["tables"].update(top_tables)

    # ------------------------------------------------------------------
    # REGION LABEL DASHBOARD
    # ------------------------------------------------------------------

    if level == "region":

        ensure_warehouse_join(joins)
        join_sql_base = "\n".join(joins)

        with engine.connect() as conn:

            # 1. Region performance table: region, total sales, total return (0 for now) --> COALESCE(SUM(ih.return_total), 0) AS total_return
            sql = f"""
                SELECT
                    r.region_name,
                    {value_expr} AS value,
                    0 AS total_return
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                FROM item_uoms
                GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                {join_sql_base}
                JOIN tbl_region r ON r.id = w.region_id
                WHERE {where_sql}
                GROUP BY r.region_name
                ORDER BY value DESC
            """
            rows = conn.execute(text(sql), params).fetchall()
            out["tables"]["region_performance"] = [dict(r._mapping) for r in rows]

            # 2. Sales by contribution pie chart (count of customers per region)
            sql = f"""
                WITH region_item_sales AS (
                    SELECT
                        r.region_name,
                        it.name AS item_name,
                        {value_expr} AS value,
                        ROW_NUMBER() OVER (
                            PARTITION BY r.region_name
                            ORDER BY {value_expr} DESC
                        ) AS rn
                    FROM invoice_headers ih
                    JOIN invoice_details id ON id.header_id = ih.id
                    JOIN items it ON it.id = id.item_id
                    LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                    ) iu ON iu.item_id = id.item_id
                    {join_sql_base}
                    JOIN tbl_region r ON r.id = w.region_id
                    WHERE {where_sql}
                    GROUP BY r.region_name, it.name
                )
                SELECT region_name, item_name, value
                FROM region_item_sales
                WHERE rn = 1
                ORDER BY value DESC
            """
            rows = conn.execute(text(sql), params).fetchall()
            out["charts"]["region_contribution_top_item"] = [
                dict(r._mapping) for r in rows
            ]

            # 3. Region wise visited customer performance line chart
            sql = f"""
                WITH total_customers AS (
                    SELECT DISTINCT
                        r.id AS region_id,
                        r.region_name,
                        cst.id AS customer_id
                    FROM agent_customers cst
                    JOIN tbl_warehouse w ON w.id = cst.warehouse
                    JOIN tbl_region r ON r.id = w.region_id
                    WHERE
                        cst.status = 1
                        AND r.id = ANY(:region_ids)
                ),

                visited_customers AS (
                    SELECT DISTINCT
                        r.id AS region_id,
                        ih.customer_id
                    FROM invoice_headers ih
                    JOIN invoice_details id ON id.header_id = ih.id
                    JOIN agent_customers cst ON cst.id = ih.customer_id
                    JOIN tbl_warehouse w ON w.id = ih.warehouse_id
                    JOIN tbl_region r ON r.id = w.region_id
                    WHERE
                        cst.status = 1
                        AND id.item_total > 0
                        AND ih.invoice_date BETWEEN :from_date AND :to_date
                        AND r.id = ANY(:region_ids)
                )

                SELECT
                    t.region_name,
                    COUNT(DISTINCT v.customer_id) AS visited_customers,
                    COUNT(DISTINCT t.customer_id) AS total_customers,
                    ROUND(
                        (COUNT(DISTINCT v.customer_id)::numeric
                        / NULLIF(COUNT(DISTINCT t.customer_id), 0)) * 100,
                        2
                    ) AS visited_percentage
                FROM total_customers t
                LEFT JOIN visited_customers v
                    ON t.customer_id = v.customer_id
                    AND t.region_id = v.region_id
                GROUP BY t.region_id, t.region_name
                ORDER BY t.region_name;
            """

            rows = conn.execute(text(sql), params).fetchall()
            out["charts"]["region_visited_customer_trend"] = [
                dict(r._mapping) for r in rows
            ]

            # 4. Region wise sales trend line chart
            sql = f"""
                SELECT
                    {period_label_sql} AS period,
                    r.region_name,
                    {value_expr} AS value
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                FROM item_uoms
                GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                {join_sql_base}
                JOIN tbl_region r ON r.id = w.region_id
                WHERE {where_sql}
                GROUP BY period, r.region_name,{order_by_sql}
                ORDER BY {order_by_sql}, r.region_name
            """
            rows = conn.execute(text(sql), params).fetchall()
            out["charts"]["region_sales_trend"] = [dict(r._mapping) for r in rows]

            joins_ws = joins.copy()
            ensure_warehouse_join(joins_ws)
            join_sql_ws = "\n".join(joins_ws)

        with engine.connect() as conn:

            top_tables = get_top_tables(
                conn,
                value_expr=value_expr,
                where_sql=where_sql,
                params=params,
                join_sql_base=join_sql_base,
                join_sql_ws=join_sql_ws,
                limit=10,
            )

        out["tables"].update(top_tables)

    # ------------------------------------------------------------------
    # AREA LABEL DASHBOARD (mirrors region logic, but grouped by area)
    # ------------------------------------------------------------------

    if level == "area":

        ensure_warehouse_join(joins)
        join_sql_base = "\n".join(joins)

        with engine.connect() as conn:

            #  Area performance table
            sql = f"""
                SELECT
                    a.area_name,
                    {value_expr} AS value,
                    0 AS total_return
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                FROM item_uoms
                GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                {join_sql_base}
                JOIN tbl_areas a ON a.id = w.area_id
                WHERE {where_sql}
                GROUP BY a.area_name
                ORDER BY value DESC
            """
            rows = conn.execute(text(sql), params).fetchall()
            out["tables"]["area_performance"] = [dict(r._mapping) for r in rows]

            # area sales by contribution 
            sql = f"""
                WITH area_item_sales AS (
                    SELECT
                        a.area_name,
                        it.name AS item_name,
                        {value_expr} AS value,
                        ROW_NUMBER() OVER (
                            PARTITION BY a.area_name
                            ORDER BY {value_expr} DESC
                        ) AS ar
                    FROM invoice_headers ih
                    JOIN invoice_details id ON id.header_id = ih.id
                    JOIN items it ON it.id = id.item_id
                    LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                    ) iu ON iu.item_id = id.item_id
                    {join_sql_base}
                    JOIN tbl_areas a ON a.id = w.area_id
                    WHERE {where_sql}
                    GROUP BY a.area_name, it.name
                )
                SELECT area_name, item_name, value
                FROM area_item_sales
                WHERE ar = 1
                ORDER BY value DESC
            """
            rows = conn.execute(text(sql), params).fetchall()
            out["charts"]["area_contribution_top_item"] = [
                dict(r._mapping) for r in rows
            ]
            #  Area wise visited customer performance
            sql = f"""
                WITH total_customers AS (
                    SELECT DISTINCT
                        a.id AS area_id,
                        a.area_name,
                        cst.id AS customer_id
                    FROM agent_customers cst
                    JOIN tbl_warehouse w ON w.id = cst.warehouse
                    JOIN tbl_areas a ON a.id = w.area_id
                    WHERE 
                        cst.status = 1
                        AND a.id = ANY(:area_ids)
                ),


                visited_customers AS (
                    SELECT DISTINCT
                        a.id AS area_id,
                        a.area_name,
                        ih.customer_id
                    FROM invoice_headers ih
                    JOIN invoice_details id ON id.header_id = ih.id
                    JOIN agent_customers cst ON cst.id = ih.customer_id
                    JOIN tbl_warehouse w ON w.id = ih.warehouse_id
                    JOIN tbl_areas a ON a.id = w.area_id
                    WHERE 
                        cst.status = 1
                        AND id.item_total > 0
                        AND ih.invoice_date BETWEEN :from_date AND :to_date
                        AND a.id = ANY(:area_ids)
                )

                SELECT
                    t.area_name,
                    COUNT(DISTINCT v.customer_id) AS visited_customers,
                    COUNT(DISTINCT t.customer_id) AS total_customers,
                    CASE 
                        WHEN COUNT(DISTINCT t.customer_id) > 0 THEN 
                            ROUND(
                                (COUNT(DISTINCT v.customer_id)::decimal / COUNT(DISTINCT t.customer_id)) * 100,
                                2
                            )
                        ELSE 0
                    END AS visited_percentage
                FROM total_customers t
                LEFT JOIN visited_customers v
                    ON t.customer_id = v.customer_id
                    AND t.area_id = v.area_id
                GROUP BY t.area_name,t.area_id
                ORDER BY t.area_name;
            """

            rows = conn.execute(text(sql), params).fetchall()
            out["charts"]["area_visited_customer_trend"] = [
                dict(r._mapping) for r in rows
            ]

            #  Area wise sales trend 
            sql = f"""
                SELECT
                    {period_label_sql} AS period,
                    a.area_name,
                    {value_expr} AS value
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                {join_sql_base}
                JOIN tbl_areas a ON a.id = w.area_id
                WHERE {where_sql}
                GROUP BY period, a.area_name,{order_by_sql}
                ORDER BY {order_by_sql}, a.area_name
            """
            rows = conn.execute(text(sql), params).fetchall()
            out["charts"]["area_sales_trend"] = [dict(r._mapping) for r in rows]

            joins_ws = joins.copy()
            ensure_warehouse_join(joins_ws)
            join_sql_ws = "\n".join(joins_ws)

        with engine.connect() as conn:

            top_tables = get_top_tables(
                conn,
                value_expr=value_expr,
                where_sql=where_sql,
                params=params,
                join_sql_base=join_sql_base,
                join_sql_ws=join_sql_ws,
                limit=10,
            )

        out["tables"].update(top_tables)

   
    # Warehouse level dashboard
    if level == "warehouse":

        selected_warehouse_count = len(filters.warehouse_ids or [])

        joins_wh = joins.copy()
        ensure_warehouse_join(joins_wh)
        joins_wh = list(dict.fromkeys(joins_wh))
        join_sql_wh = "\n".join(joins_wh)

        sql = f"""
            SELECT
                (w.warehouse_code || ' - ' || w.warehouse_name) AS label,
                w.address AS location,
                {value_expr} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {join_sql_wh}
            WHERE {where_sql}
            GROUP BY w.warehouse_code, w.warehouse_name, w.address
            ORDER BY value DESC
        """
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        warehouse_data = [dict(r._mapping) for r in rows]

        # 1) REGION-WISE SALES CONTRIBUTION 

        sql = f"""
            WITH sales AS (
                SELECT
                    rg.region_name,
                    {value_expr} AS value
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                {join_sql_wh}
                JOIN tbl_areas a ON a.id = w.area_id
                JOIN tbl_region rg ON rg.id = a.region_id
                WHERE {where_sql}
                GROUP BY rg.region_name
            )
            SELECT region_name, value,
                ROUND(((value/ SUM(value) OVER()) * 100)::numeric, 2) AS percentage
            FROM sales
            ORDER BY value DESC;
            """

        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        out["charts"]["region_contribution"] = [dict(r._mapping) for r in rows]

        # 2) AREA-WISE SALES CONTRIBUTION
        sql = f"""
            WITH sales AS (
                SELECT
                    a.area_name,
                    {value_expr} AS value
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                {join_sql_wh}
                JOIN tbl_areas a ON a.id = w.area_id
                WHERE {where_sql}
                GROUP BY a.area_name
            )
            SELECT area_name, value,
                ROUND(((value / SUM(value) OVER()) * 100)::numeric, 2) AS percentage
            FROM sales
            ORDER BY value DESC;
            """

        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        out["charts"]["area_contribution"] = [dict(r._mapping) for r in rows]

        # 3) WAREHOUSE SALES PIE/TABLE (same logic you had)

        if 1 <= selected_warehouse_count <= 10:
            out["charts"]["warehouse_sales"] = [
                {"name": r["label"], "value": r["value"]} for r in warehouse_data
            ]
            out["tables"]["warehouse_sales"] = []
        else:
            out["charts"]["warehouse_sales"] = []
            out["tables"]["warehouse_sales"] = warehouse_data

        # 4) WAREHOUSE TREND 

        sql = f"""
            SELECT
                {period_label_sql} AS period,
                (w.warehouse_code || ' - ' || w.warehouse_name) AS warehouse_label,
                {value_expr} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {join_sql_wh}
            WHERE {where_sql}
            GROUP BY period,w.warehouse_code, w.warehouse_name,{order_by_sql}
            ORDER BY {order_by_sql}, w.warehouse_name
        """
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        out["charts"]["warehouse_trend"] = [dict(r._mapping) for r in rows]

        # 5) CONDITIONAL TOP TABLES BASED ON selected_warehouse_count

        # TOP 10 ITEMS (for selected warehouses)
        sql_items = f"""
                    SELECT
                        it.name AS item_name,
                        {value_expr} AS value
                    FROM invoice_headers ih
                    JOIN invoice_details id ON id.header_id = ih.id
                    JOIN items it ON it.id = id.item_id
                    LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                    ) iu ON iu.item_id = id.item_id
                    {join_sql_wh}
                    WHERE {where_sql}
                    GROUP BY it.name
                    ORDER BY value DESC
                    LIMIT 10
                """
        with engine.connect() as conn:
            rows = conn.execute(text(sql_items), params).fetchall()
        out["tables"]["top_items"] = [dict(r._mapping) for r in rows]

        # TOP 10 CUSTOMERS (for selected warehouses)
        sql_cust = f"""
            SELECT
                cst.id AS customer_id,
                cst.osa_code || ' - ' || cst.name AS customer_name,
                cst.contact_no AS contact,
                w.warehouse_code || ' - ' || w.warehouse_name AS warehouse_label,
                {value_expr} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN agent_customers cst ON cst.id = ih.customer_id
            LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {join_sql_wh}
            WHERE {where_sql}
            GROUP BY cst.id, cst.name, cst.contact_no, w.warehouse_code, w.warehouse_name, cst.osa_code
            ORDER BY value DESC
            LIMIT 10
        """
        with engine.connect() as conn:
            rows = conn.execute(text(sql_cust), params).fetchall()
        out["tables"]["top_customers"] = [dict(r._mapping) for r in rows]

        with engine.connect() as conn:

            if 1 <= selected_warehouse_count <= 10:
                out["tables"]["top_salesmen"] = []
                out["tables"]["top_warehouses"] = []

            else:
                # TOP 10 SALESMEN (for selected warehouses)
                sql_salesmen = f"""
                    SELECT
                        s.osa_code || ' - ' || s.name AS salesman,
                        w.warehouse_code || ' - ' || w.warehouse_name AS warehouse_label,
                        {value_expr} AS value
                    FROM invoice_headers ih
                    JOIN invoice_details id ON id.header_id = ih.id
                    JOIN salesman s ON s.id = ih.salesman_id
                    LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                    ) iu ON iu.item_id = id.item_id
                    {join_sql_wh}
                    WHERE {where_sql}
                    GROUP BY s.osa_code, s.name, w.warehouse_code, w.warehouse_name
                    ORDER BY value DESC
                    LIMIT 10
                """
                rows = conn.execute(text(sql_salesmen), params).fetchall()
                out["tables"]["top_salesmen"] = [dict(r._mapping) for r in rows]

                # TOP 10 WAREHOUSES (within the selected warehouse set) - shows top performing warehouses
                sql_wh_top = f"""
                    SELECT
                        w.warehouse_code || ' - ' || w.warehouse_name AS warehouse_label,
                        w.address AS location,
                        {value_expr} AS value
                    FROM invoice_headers ih
                    JOIN invoice_details id ON id.header_id = ih.id
                    LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                    ) iu ON iu.item_id = id.item_id
                    {join_sql_wh}
                    WHERE {where_sql}
                    GROUP BY w.warehouse_code, w.warehouse_name, w.address
                    ORDER BY value DESC
                    LIMIT 10
                """
                rows = conn.execute(text(sql_wh_top), params).fetchall()
                out["tables"]["top_warehouses"] = [dict(r._mapping) for r in rows]

    return out
