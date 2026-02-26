from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.sales_report.schemas.sales_schema import FilterSelection

from app.sales_report.utils.dashboard_helper import (
    prepare_dashboard_context,
    detect_level,
)
from app.database import engine

router = APIRouter(tags=["sales dashboard company level"])


@router.post("/company-wise-sales")
def company_wise_sales(filters: FilterSelection):

    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)

    if level != "company":
        raise HTTPException(status_code=400, detail="company level required")

    out = {"charts": {}}
    with engine.connect() as conn:
        query = f"""
            SELECT
                c.company_name,
                {ctx['value_expr']} as value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN tbl_company c ON c.id = ih.company_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {ctx['join_sql']}
            WHERE {ctx['where_sql']}
            GROUP BY c.company_name
            ORDER BY value DESC
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["charts"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/region-wise-sales")
def region_wise_sales(filters: FilterSelection):

    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)

    if level != "company":
        raise HTTPException(status_code=400, detail="company level required")

    out = {"charts": {}}
    with engine.connect() as conn:
        query = f"""
            SELECT
                r.region_name,
                {ctx['value_expr']} as value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN tbl_warehouse w ON w.id = ih.warehouse_id
            JOIN tbl_region r ON r.id = w.region_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {ctx['join_sql']}
            WHERE {ctx['where_sql']}
            GROUP BY r.region_name
            ORDER BY value DESC
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["charts"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/area-wise-sales")
def area_wise_sales(filters: FilterSelection):

    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)

    if level != "company":
        raise HTTPException(status_code=400, detail="company level required")

    out = {"charts": {}}
    with engine.connect() as conn:
        query = f"""
            SELECT
                a.area_name,
                {ctx['value_expr']} as value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN tbl_warehouse w ON w.id = ih.warehouse_id
            JOIN tbl_areas a ON a.id = w.area_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {ctx['join_sql']}
            WHERE {ctx['where_sql']}
            GROUP BY a.area_name
            ORDER BY value DESC
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["charts"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/company-trendline-sales")
def company_trendline_sales(filters: FilterSelection):

    ctx = prepare_dashboard_context(filters)
    level = detect_level(filters)

    if level != "company":
        raise HTTPException(status_code=400, detail="company level required")

    out = {"granularity": ctx["granularity"], "charts": {}}
    with engine.connect() as conn:
        query = f"""
            SELECT
                {ctx['period_label_sql']} AS period,
                c.company_name,
                {ctx['value_expr']} as value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN tbl_company c ON c.id = ih.company_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
           {ctx['join_sql']}
            WHERE {ctx['where_sql']}
            GROUP BY period, c.company_name,{ctx['order_by_sql']}
            ORDER BY {ctx['order_by_sql']}, c.company_name 
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["charts"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/top_salesman")
def top_salesman(filters: FilterSelection):
    ctx = prepare_dashboard_context(filters)

    out = {"top_salesmen": {}}
    with engine.connect() as conn:
        query = f"""
            SELECT
            s.osa_code || ' - ' || s.name AS salesman,
            w.warehouse_name,
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
            GROUP BY s.osa_code, s.name, w.warehouse_name
            ORDER BY value DESC
            LIMIT 10
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["top_salesmen"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/top_warehouse")
def top_warehouse(filters: FilterSelection):
    ctx = prepare_dashboard_context(filters)

    out = {"top_warehouse": {}}
    with engine.connect() as conn:
        query = f"""
            SELECT
            w.warehouse_code || ' - ' || w.warehouse_name AS warehouse_name,
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
            LIMIT 10
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["top_warehouse"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/top_items")
def top_items(filters: FilterSelection):
    ctx = prepare_dashboard_context(filters)

    out = {"top_items": {}}
    with engine.connect() as conn:
        query = f"""
            SELECT
            it.name AS item_name,
            {ctx['value_expr']} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN items it ON it.id = id.item_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {ctx['join_sql']}
            WHERE {ctx['where_sql']}
            GROUP BY it.name
            ORDER BY value DESC
            LIMIT 10
            """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["top_items"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/top_customer")
def top_customer(filters: FilterSelection):
    ctx = prepare_dashboard_context(filters)

    out = {"top_items_customer": {}}
    with engine.connect() as conn:
        query = f"""
           SELECT
            w.warehouse_code || ' - ' || w.warehouse_name AS warehouse_name,
            cst.osa_code || ' - ' || cst.name AS customer_name,
            cst.contact_no AS contact,
            {ctx['value_expr']} AS value
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN agent_customers cst ON cst.id = ih.customer_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {ctx['join_sql']}
            WHERE {ctx['where_sql']}
            GROUP BY
            cst.id,
            cst.osa_code,
            cst.name,
            cst.contact_no,
            w.warehouse_name,
            w.warehouse_code
            ORDER BY value DESC
            LIMIT 10
        """
        rows = conn.execute(text(query), ctx["params"]).fetchall()
        out["top_customer"] = [dict(r._mapping) for r in rows]

    return out


