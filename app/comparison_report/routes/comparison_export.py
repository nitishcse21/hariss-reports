
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from datetime import datetime
import xlsxwriter
import io
from app.database import engine
from app.comparison_report.schemas.comparison_schema import ComparisonRequest
from app.comparison_report.utils.comparison_common_helper import (
    validate_mandatory,
    build_query_parts,
    quantity_expr_sql,
    get_periods,
)

router = APIRouter()


@router.post("/comparison-export")
def comparison_export(filters: ComparisonRequest):
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

    sql = f"""
        SELECT
            i.code || '_' || i.name AS item_name,
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
        GROUP BY i.code, i.name
        ORDER BY i.code, i.name
    """

    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        rows = [dict(r._mapping) for r in result]

    current_label = f"{current_from:%b %d, %Y} – {current_to:%b %d, %Y}"
    prev_label = f"{prev_from:%b %d, %Y} – {prev_to:%b %d, %Y}"

    # ========== STREAM EXCEL ==========
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    worksheet = workbook.add_worksheet("Comparison")

    headers = [
        "Item Name",
        "Current Period",
        "Previous Period",
        "Current Sales",
        "Previous Sales",
        "Difference",
        "Growth %",
    ]

    header_fmt = workbook.add_format({"bold": True})

    for col, h in enumerate(headers):
        worksheet.write(0, col, h, header_fmt)

    row_no = 1
    for r in rows:
        curr = float(r["current_sales"] or 0)
        prev = float(r["previous_sales"] or 0)
        growth = 0 if prev == 0 else round(((curr - prev) / prev) * 100, 2)

        worksheet.write_row(row_no, 0, [
            r["item_name"],
            current_label,
            prev_label,
            curr,
            prev,
            round(curr - prev, 3),
            growth
        ])
        row_no += 1

    worksheet.autofilter(0, 0, row_no, len(headers) - 1)
    worksheet.set_column(0, 0, 30)
    worksheet.set_column(1, 2, 25)
    worksheet.set_column(3, 6, 18)

    workbook.close()
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=comparison_report.xlsx"
        },
    )
