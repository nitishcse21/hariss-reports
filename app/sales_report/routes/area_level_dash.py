from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.sales_report.schemas.sales_schema import FilterSelection

from app.sales_report.utils.dashboard_helper import (
    prepare_dashboard_context,
    detect_level,
)
from app.database import engine

router = APIRouter(tags=["sales dashboard area level"])

@router.post("/area-performance")
def area_performance(filters:FilterSelection):
    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "area":
        raise HTTPException(status_code=400, detail="area level required")
    
    out = {"area_perfomance":{}}

    with engine.connect() as conn:
        query = f"""
                 SELECT
                    a.area_name,
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
                JOIN tbl_areas a ON a.id = w.area_id
                WHERE {ctx['where_sql']}
                GROUP BY a.area_name
                ORDER BY value DESC
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["area_perfomance"] = [dict(r._mapping) for r in rows]
    return out


@router.post("/area-contribution-top-items")
def area_contribution(filters:FilterSelection):
    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "area":
        raise HTTPException(status_code=400, detail="area level required")
    
    out = {"area_contribution":{}}

    with engine.connect() as conn:
        query = f"""
                WITH area_item_sales AS (
                    SELECT
                        a.area_name,
                        it.name AS item_name,
                        {ctx['value_expr']} AS value,
                        ROW_NUMBER() OVER (
                            PARTITION BY a.area_name
                            ORDER BY {ctx['value_expr']} DESC
                        ) AS ar
                    FROM invoice_headers ih
                    JOIN invoice_details id ON id.header_id = ih.id
                    JOIN items it ON it.id = id.item_id
                    LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                    ) iu ON iu.item_id = id.item_id
                    {ctx['join_sql']}
                    JOIN tbl_areas a ON a.id = w.area_id
                    WHERE {ctx['where_sql']}
                    GROUP BY a.area_name, it.name
                )
                SELECT area_name, item_name, value
                FROM area_item_sales
                WHERE ar = 1
                ORDER BY value DESC
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["area_contribution"] = [dict(r._mapping) for r in rows]
    return out



@router.post("/area-wise-visited-customer-performance")
def area_wise_visited_customer(filters:FilterSelection):
    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "area":
        raise HTTPException(status_code=400, detail="area level required")
    
    out = {"area_contribution":{}}

    with engine.connect() as conn:
        query = f"""
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
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["area_contribution"] = [dict(r._mapping) for r in rows]
    return out



@router.post("/area-trendline-sales")
def area_trendline_sales(filters:FilterSelection):
    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "area":
        raise HTTPException(status_code=400, detail="area level required")
    
    out = {"granularity":{ctx['granularity']},"area_trendline":{}}

    with engine.connect() as conn:
        query = f"""
               SELECT
                    {ctx['period_label_sql']} AS period,
                    a.area_name,
                    {ctx['value_expr']} AS value
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                {ctx['join_sql']}
                JOIN tbl_areas a ON a.id = w.area_id
                WHERE {ctx['where_sql']}
                GROUP BY period, a.area_name,{ctx['order_by_sql']}
                ORDER BY {ctx['order_by_sql']}, a.area_name
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["area_trendline"] = [dict(r._mapping) for r in rows]
    return out


