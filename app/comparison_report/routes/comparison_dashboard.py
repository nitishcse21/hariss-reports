from fastapi import APIRouter
from sqlalchemy import text
from datetime import datetime
from app.database import engine
from app.comparison_report.schemas.comparison_schema import ComparisonRequest
from app.comparison_report.utils.comparison_common_helper import (
    get_periods,
    build_query_parts,
    quantity_expr_sql,
    validate_mandatory,
)

router = APIRouter()


@router.post("/dashboard")
def comparison_dashboard(filters: ComparisonRequest):
    validate_mandatory(filters)

    selected_date = filters.selected_date
    if isinstance(selected_date, str):
        selected_date = datetime.strptime(selected_date, "%Y-%m-%d").date()


    current_from, current_to, prev_from, prev_to = get_periods(
        filters.report_by, selected_date
    )

    joins, where_fragments, params = build_query_parts(
        filters, prev_from, current_to
    )

    params.update({
        "current_from": current_from,
        "current_to": current_to,
        "prev_from": prev_from,
        "prev_to": prev_to,
    })

    join_sql = "\n".join(joins)
    where_sql = " AND ".join(where_fragments)

    current_cond = "ih.invoice_date BETWEEN :current_from AND :current_to"
    prev_cond = "ih.invoice_date BETWEEN :prev_from AND :prev_to"

    if filters.search_type.lower() == "quantity":
        current_expr = quantity_expr_sql(current_cond)
        prev_expr = quantity_expr_sql(prev_cond)
    else:
        current_expr = f"SUM(CASE WHEN {current_cond} THEN id.item_total ELSE 0 END)"
        prev_expr = f"SUM(CASE WHEN {prev_cond} THEN id.item_total ELSE 0 END)"

    if filters.report_by == "year":
        bucket = "TO_CHAR(ih.invoice_date, 'YYYY-MM')"
    else:
        bucket = "DATE(ih.invoice_date)"



    top_categories_sql = f"""
        SELECT
        ic.category_name AS item_category_name,
            {current_expr} AS current_sales,
            {prev_expr} AS previous_sales
        FROM invoice_headers ih
        JOIN invoice_details id ON id.header_id = ih.id
        JOIN items i ON i.id = id.item_id
        JOIN item_categories ic ON ic.id = i.category_id
        LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
        ) iu ON iu.item_id = id.item_id
        {join_sql}
        WHERE {where_sql}
        GROUP BY ic.category_name
        ORDER BY current_sales DESC
        LIMIT 5
        """


    top_items_sql = f"""
        SELECT
            i.code || '-' || i.name AS item_name,
            {current_expr} AS current_sales,
            {prev_expr} AS previous_sales
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
        GROUP BY i.name, i.code
        ORDER BY current_sales DESC
        LIMIT 5
        """

    trend_sql = f"""
        SELECT
            {bucket} AS period,
            {current_expr} AS current_sales,
            {prev_expr} AS previous_sales
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
        GROUP BY period
        ORDER BY period
        """
    
    with engine.connect() as conn:
        top_categories = [dict(r._mapping) for r in conn.execute(text(top_categories_sql), params)]
        top_items = [dict(r._mapping) for r in conn.execute(text(top_items_sql), params)]
        trend = [dict(r._mapping) for r in conn.execute(text(trend_sql), params)]

    return {
        "top_categories_current": top_categories,
        "top_items_current": top_items,
        "trend": trend,
    }