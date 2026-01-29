from fastapi import APIRouter, Query, Request
from sqlalchemy import text
from datetime import datetime
from app.database import engine
from app.comparison_report.schemas.comparison_schema import ComparisonRequest
from app.comparison_report.utils.comparison_common_helper import (
    validate_mandatory,
    build_query_parts,
    quantity_expr_sql,
    get_periods,
)

router = APIRouter()

ROWS_PER_PAGE = 50

@router.post("/comparison-table")
def comparison_table(filters: ComparisonRequest, request: Request,
    page: int = Query(1, ge=1)):
    validate_mandatory(filters)

    selected_date = filters.selected_date
    if isinstance(selected_date, str):
        selected_date = datetime.strptime(selected_date, "%Y-%m-%d").date()

    current_from, current_to, prev_from, prev_to = get_periods(
        filters.report_by, selected_date
    )

    joins, where_fragments, params = build_query_parts(filters, prev_from, current_to)

    current_cond = "ih.invoice_date BETWEEN :current_from AND :current_to"
    prev_cond = "ih.invoice_date BETWEEN :prev_from AND :prev_to"

    params.update(
        {
            "current_from": current_from,
            "current_to": current_to,
            "prev_from": prev_from,
            "prev_to": prev_to,
        }
    )

    if filters.search_type.lower() == "quantity":
        current_expr = quantity_expr_sql(current_cond)
        prev_expr = quantity_expr_sql(prev_cond)
    else:
        current_expr = f"SUM(CASE WHEN {current_cond} THEN id.item_total ELSE 0 END)"
        prev_expr = f"SUM(CASE WHEN {prev_cond} THEN id.item_total ELSE 0 END)"

    join_sql = "\n".join(joins)
    where_sql = " AND ".join(where_fragments)

    base_sql = f"""
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN items i ON i.id = id.item_id
            LEFT JOIN (
                SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                FROM item_uoms
                GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            {join_sql}
            WHERE {where_sql}
        """
    
    count_sql = f"SELECT COUNT(*) {base_sql}"
    with engine.connect() as conn:
        total_rows = conn.execute(text(count_sql), params).scalar()

    offset = (page - 1) * ROWS_PER_PAGE
    params["limit"] = ROWS_PER_PAGE
    params["offset"] = offset


    sql = f"""
        SELECT
            i.code || '_' || i.name AS item_name,
            {current_expr} AS current_sales,
            {prev_expr} AS previous_sales
        {base_sql}
        GROUP BY i.code, i.name
        ORDER BY i.code, i.name
        LIMIT :limit OFFSET :offset
        """
    

    with engine.connect() as conn:
        result = conn.execute(text(sql), params)

    rows = [dict(r._mapping) for r in result]
    total_pages = (total_rows + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE
    base_url = str(request.url).split("?")[0]

    current_label = f"{current_from:%b %d, %Y} – {current_to:%b %d, %Y}"
    prev_label = f"{prev_from:%b %d, %Y} – {prev_to:%b %d, %Y}"

    data = []
    for r in rows:
        curr = float(r["current_sales"] or 0)
        prev = float(r["previous_sales"] or 0)
        growth = 0 if prev == 0 else round(((curr - prev) / prev) * 100, 2)

        data.append(
            {
                "item_name": r["item_name"],
                "current_period": current_label,
                "previous_period": prev_label,
                "current_sales": curr,
                "previous_sales": prev,
                "difference": round(curr - prev, 3),
                "growth_percent": growth,
            }
        )

    return {
        "total_rows": total_rows,
        "total_pages": total_pages,
        "current_page": page,
        "next_page": f"{base_url}?page={page + 1}" if page < total_pages else None,
        "previous_page": f"{base_url}?page={page - 1}" if page > 1 else None,
        "data": data
        }
