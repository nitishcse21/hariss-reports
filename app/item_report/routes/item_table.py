from app.item_report.schemas.item_schema import FilterSelection
from app.item_report.utils.item_common_helper import validate_mandatory, build_query_parts, quantity_expr_sql
from math import ceil
from fastapi import APIRouter, Query, Request
from sqlalchemy import text
from app.database import engine
import time

router = APIRouter()

ROWS_PER_PAGE = 50


@router.post("/item_table")
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

    if filters.search_type.lower() == "quantity":
            value_expr = quantity_expr_sql()

    else:
            value_expr = "SUM(id.item_total)"

    offset = (page - 1) * ROWS_PER_PAGE

    # ===========================
    # SINGLE AGGREGATION CTE
    # ===========================
    sql = f"""
    WITH aggregated AS (
        SELECT
            i.code || ' - ' || i.name AS item_name,
            {value_expr} AS value
        FROM invoice_headers ih
        JOIN invoice_details id ON id.header_id = ih.id
        JOIN items i ON i.id = id.item_id
        LEFT JOIN (
    SELECT
        item_id,
        MAX(NULLIF(upc::numeric, 0)) AS upc
    FROM item_uoms
    GROUP BY item_id
) iu ON iu.item_id = id.item_id

        {join_sql}
        WHERE {where_sql}
        GROUP BY i.code, i.name
    )
    SELECT *,
           COUNT(*) OVER() AS total_rows
    FROM aggregated
    ORDER BY value DESC
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