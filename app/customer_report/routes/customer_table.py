from app.customer_report.schemas.customer_sales_schema import FilterSelection
from app.customer_report.utils.customer_common_helper import validate_mandatory, build_query_parts
from math import ceil
from fastapi import APIRouter, Query, Request
from sqlalchemy import text
from app.database import engine
import time

router = APIRouter()

ROWS_PER_PAGE = 50

@router.post("/customer-sales-table")
def get_table(
    filters: FilterSelection,
    request: Request,
    page: int = Query(1, ge=1)
):
    start = time.time()
    validate_mandatory(filters)

    joins, where_fragments, params = build_query_parts(filters)
    join_sql = "\n".join(joins)
    where_sql = " AND ".join(where_fragments)

    value_expr = (
        "SUM(id.quantity)"
        if filters.search_type.lower() == "quantity"
        else "SUM(id.item_total)"
    )

    offset = (page - 1) * ROWS_PER_PAGE

    # ===========================
    # SINGLE AGGREGATION CTE
    # ===========================
    sql = f"""
    WITH aggregated AS (
        SELECT
            ac.osa_code || '-' || ac.name AS customer_name,
            ac.contact_no AS mobile_number,
            w.warehouse_code || '-' || w.warehouse_name AS warehouse,
            tr.route_code || '-' || tr.route_name AS route,
            oc.outlet_channel_code || '-' || oc.outlet_channel AS customer_channel,
            cat.customer_category_code || '-' || cat.customer_category_name AS customer_category,
            {value_expr} AS value
        FROM invoice_headers ih
        JOIN invoice_details id ON id.header_id = ih.id
        JOIN agent_customers ac ON ac.id = ih.customer_id
        JOIN outlet_channel oc ON oc.id = ac.outlet_channel_id
        JOIN customer_categories cat ON cat.id = ac.category_id
        JOIN tbl_route tr ON tr.id = ih.route_id
        {join_sql}
        WHERE {where_sql}
        GROUP BY
            ac.osa_code, ac.name, ac.contact_no,
            w.warehouse_code, w.warehouse_name,
            tr.route_code, tr.route_name,
            oc.outlet_channel_code, oc.outlet_channel,
            cat.customer_category_code, cat.customer_category_name
    )
    SELECT *,
           COUNT(*) OVER() AS total_rows
    FROM aggregated
    ORDER BY customer_name, warehouse, route
    LIMIT {ROWS_PER_PAGE} OFFSET {offset}
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    
    
    total_rows = rows[0]._mapping["total_rows"] if rows else 0
    total_pages = max(ceil(total_rows / ROWS_PER_PAGE), 1)
    base_url = str(request.url).split("?")[0]

    rows_data = [dict(r._mapping) for r in rows]
    for r in rows_data:
        r.pop("total_rows", None)


    end = time.time()
    print(f"FAST query took {end - start:.2f} seconds")

    return {
        "total_rows": total_rows,
        "total_pages": total_pages,
        "current_page": page,
        "next_page": f"{base_url}?page={page + 1}" if page < total_pages else None,
        "previous_page": f"{base_url}?page={page - 1}" if page > 1 else None,
        "rows": rows_data,
    }