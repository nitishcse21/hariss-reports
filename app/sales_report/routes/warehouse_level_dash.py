from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.sales_report.schemas.sales_schema import FilterSelection

from app.sales_report.utils.dashboard_helper import (
    prepare_dashboard_context,
    detect_level,
)
from app.database import engine

router = APIRouter(tags=["sales dashboard warehouse level"])


@router.post("/region-wise-sales-contribution")
def region_wise_sales_contribution(filters:FilterSelection):
    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "warehouse":
        raise HTTPException(status_code=400, detail="warehouse level required")
    
    out ={"region_contribution":{}}

    with engine.connect() as conn:

        query = f"""
                 WITH sales AS (
                SELECT
                    rg.region_name,
                    {ctx['value_expr']} AS value
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                {ctx['join_sql']}
                JOIN tbl_region rg ON rg.id = w.region_id
                WHERE {ctx['where_sql']}
                GROUP BY rg.region_name
            )
            SELECT region_name, value,
                ROUND(((value/ SUM(value) OVER()) * 100)::numeric, 2) AS percentage
            FROM sales
            ORDER BY value DESC;
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["region_contribution"] = [dict(r._mapping) for r in rows]
        return out


@router.post("/area-wise-sales-contribution")
def area_wise_sales_contribution(filters:FilterSelection):
    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "warehouse":
        raise HTTPException(status_code=400, detail="warehouse level required")
    
    out ={"area_contribution":{}}

    with engine.connect() as conn:

        query = f"""
                 WITH sales AS (
                SELECT
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
                GROUP BY a.area_name
            )
            SELECT area_name, value,
                ROUND(((value / SUM(value) OVER()) * 100)::numeric, 2) AS percentage
            FROM sales
            ORDER BY value DESC;
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["area_contribution"] = [dict(r._mapping) for r in rows]
        return out



@router.post("/warehouse-trendline-sales")
def warehouse_trendlinne_sales(filters:FilterSelection):
    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "warehouse":
        raise HTTPException(status_code=400, detail="warehouse level required")
    
    out ={"granularity":{ctx['granularity']},"warehouse_trendline":{}}

    with engine.connect() as conn:

        query = f"""
                SELECT
                {ctx['period_label_sql']} AS period,
                (w.warehouse_code || ' - ' || w.warehouse_name) AS warehouse_label,
                {ctx['value_expr']} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {ctx['join_sql']}
            WHERE {ctx['where_sql']}
            GROUP BY period,w.warehouse_code, w.warehouse_name,{ctx['order_by_sql']}
            ORDER BY {ctx['order_by_sql']}, w.warehouse_name
        """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["warehouse_trendline"] = [dict(r._mapping) for r in rows]
        return out
    


@router.post("/top-salesman-and-warehouse")
def top_salesman_and_warehouse(filters:FilterSelection):
    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)
    if level != "warehouse":
        raise HTTPException(status_code=400, detail="warehouse level required")
    selected_warehouse_count = len(filters.warehouse_ids or [])
    out = {"charts":{}, "table":{}}

    with engine.connect() as conn:

        query = f"""
                SELECT
                (w.warehouse_code || ' - ' || w.warehouse_name) AS label,
                w.address AS location,
                {ctx['value_expr']} AS value
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                    SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                    FROM item_uoms
                    GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
            {ctx['join_sql']}
            WHERE {ctx['where_sql']}
            GROUP BY w.warehouse_code, w.warehouse_name, w.address
            ORDER BY value DESC
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        warehouse_data = [dict(r._mapping) for r in rows]

        sql = f"""
                SELECT
                    s.osa_code || ' - ' || s.name AS salesman,
                    w.warehouse_code || ' - ' || w.warehouse_name AS warehouse_label,
                    {ctx['value_expr']} AS value
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                JOIN salesman s ON s.id = ih.salesman_id
                LEFT JOIN (
                SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                FROM item_uoms
                GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
            {ctx['join_sql']}
            WHERE {ctx['where_sql']}
            GROUP BY s.osa_code, s.name, w.warehouse_code, w.warehouse_name
            ORDER BY value DESC
            LIMIT 10
            """
        rows = conn.execute(text(sql), ctx["params"]).fetchall()
        salesman_data = [dict(r._mapping) for r in rows]


        if 1 <= selected_warehouse_count <= 10:
            out["charts"]["warehouse_sales"] = [
                {"warehouse_name": r["label"], "value": r["value"]} for r in warehouse_data
            ] 
            out["table"]["warehouse_sales"] = []
            out["table"]["salesman_sales"] = salesman_data
        else:
            out["charts"]["warehouse_sales"] = []
            out["table"]["warehouse_sales"] = warehouse_data
            

        return out