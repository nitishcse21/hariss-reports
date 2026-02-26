from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.sales_report.schemas.sales_schema import FilterSelection

from app.sales_report.utils.dashboard_helper import (
    prepare_dashboard_context,
    detect_level,
)
from app.database import engine

router = APIRouter(tags=["sales dashboard region level"])

@router.post("/region-performance")
def region_performance(filters:FilterSelection):

    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "region":
        raise HTTPException(status_code=400, detail="region level required")
    
    out = {"region_performance":{}}

    with engine.connect() as conn:
        query = f"""
                SELECT
                    r.region_name,
                    {ctx['value_expr']} AS value,
                    0 AS total_return
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                FROM item_uoms
                GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                {ctx['join_sql']}
                JOIN tbl_region r ON r.id = w.region_id
                WHERE {ctx['where_sql']}
                GROUP BY r.region_name
                ORDER BY value DESC
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        print(rows)
        out["region_performance"] = [dict(r._mapping) for r in rows]
        return out

        

@router.post("/region-contribution-top-items")
def region_contribution(filters:FilterSelection):

    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "region":
        raise HTTPException(status_code=400, detail="region level required")
    
    out = {"region_contribution":{}}

    with engine.connect() as conn:
        query = f"""
                WITH region_item_sales AS (
                    SELECT
                        r.region_name,
                        it.name AS item_name,
                        {ctx['value_expr']} AS value,
                        ROW_NUMBER() OVER (
                            PARTITION BY r.region_name
                            ORDER BY {ctx['value_expr']} DESC
                        ) AS rn
                    FROM invoice_headers ih
                    JOIN invoice_details id ON id.header_id = ih.id
                    JOIN items it ON it.id = id.item_id
                    LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                    ) iu ON iu.item_id = id.item_id
                    {ctx['join_sql']}
                    JOIN tbl_region r ON r.id = w.region_id
                    WHERE {ctx['where_sql']}
                    GROUP BY r.region_name, it.name
                )
                SELECT region_name, item_name, value
                FROM region_item_sales
                WHERE rn = 1
                ORDER BY value DESC
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["region_contribution"] = [dict(r._mapping) for r in rows]
        return out

        

@router.post("/region-wise-visited-customer-performance")
def region_wise_visited_customer(filters:FilterSelection):

    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "region":
        raise HTTPException(status_code=400, detail="region level required")
    
    out = {"region_wise_visited_customer":{}}

    with engine.connect() as conn:
        query = f"""
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
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["region_wise_visited_customer"] = [dict(r._mapping) for r in rows]
        return out


@router.post("/region-trendline-sales")
def region_trendline_sales(filters:FilterSelection):

    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "region":
        raise HTTPException(status_code=400, detail="region level required")
    
    out = {"granularity":{ctx['granularity']},"region_trendline_sales":{}}

    with engine.connect() as conn:
        query = f"""
                SELECT
                    {ctx['period_label_sql']} AS period,
                    r.region_name,
                    {ctx['value_expr']} AS value
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                FROM item_uoms
                GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                {ctx['join_sql']}
                JOIN tbl_region r ON r.id = w.region_id
                WHERE {ctx['where_sql']}
                GROUP BY period, r.region_name,{ctx['order_by_sql']}
                ORDER BY {ctx['order_by_sql']}, r.region_name
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["region_trendline_sales"] = [dict(r._mapping) for r in rows]
        return out





        


