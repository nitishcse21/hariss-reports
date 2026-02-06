# from fastapi import APIRouter, Query, HTTPException
# from typing import Optional, Dict
# from sqlalchemy import text
# from app.database import engine
# from app.attendance_report.utils.common_helper import parse_csv_ids
# from datetime import datetime, timedelta
# from dateutil.relativedelta import relativedelta

# router = APIRouter()

# @router.get("/comparison-table")
# def comparison_report(
#     warehouse_ids: Optional[str] = Query(None),
#     salesman_ids: Optional[str] = Query(None),
#     from_date: str = Query(...),
#     to_date: str = Query(...),
#     dataview: str = Query("monthly"),
# ):

#     warehouse_ids_list = parse_csv_ids(warehouse_ids)
#     salesman_ids_list = parse_csv_ids(salesman_ids)

#     if dataview not in {"daily", "weekly", "monthly", "yearly"}:
#         raise HTTPException(
#             status_code=400,
#             detail="Invalid dataview. Allowed: daily, weekly, monthly, yearly",
#         )

#     # ---- parse dates ----
#     try:
#         from_date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
#         to_date_obj = datetime.strptime(to_date, "%Y-%m-%d").date()
#     except ValueError:
#         raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD")

#     # ---- previous range ----
#     if dataview == "daily":
#         prev_from, prev_to = (
#             from_date_obj - timedelta(days=1),
#             to_date_obj - timedelta(days=1),
#         )
#     elif dataview == "weekly":
#         prev_from, prev_to = (
#             from_date_obj - timedelta(days=7),
#             to_date_obj - timedelta(days=7),
#         )
#     elif dataview == "monthly":
#         prev_from, prev_to = (
#             from_date_obj - relativedelta(months=1),
#             to_date_obj - relativedelta(months=1),
#         )
#     else:  # yearly
#         prev_from, prev_to = (
#             from_date_obj - relativedelta(years=1),
#             to_date_obj - relativedelta(years=1),
#         )

#     try:
#         with engine.connect() as conn:

#             base_query = """
#                 SELECT
#                     idt.item_id,
#                     itm.code || ' - ' || itm.name AS item_name,
#                     SUM(idt.quantity) AS total_quantity
#                 FROM invoice_headers ih
#                 JOIN invoice_details idt
#                     ON idt.header_id = ih.id
#                 JOIN items itm
#                     ON itm.id = idt.item_id
#                 WHERE ih.invoice_date BETWEEN :from_date AND :to_date
#                 AND (
#                     (:salesman_ids IS NOT NULL AND ih.salesman_id = ANY(:salesman_ids))
#                  OR (:salesman_ids IS NULL AND :warehouse_ids IS NOT NULL AND ih.warehouse_id = ANY(:warehouse_ids))
#                  OR (:salesman_ids IS NULL AND :warehouse_ids IS NULL)
#                 )
#                 GROUP BY idt.item_id, itm.code, itm.name
#             """

#             def fetch_itemwise(f_date, t_date) -> Dict[int, dict]:
#                 rows = conn.execute(
#                     text(base_query),
#                     {
#                         "from_date": f_date,
#                         "to_date": t_date,
#                         "warehouse_ids": warehouse_ids_list,
#                         "salesman_ids": salesman_ids_list,
#                     },
#                 ).fetchall()

#                 return {
#                     r.item_id: {
#                         "item_id": r.item_id,
#                         "item_name": r.item_name,
#                         "quantity": r.total_quantity or 0,
#                     }
#                     for r in rows
#                 }

#             current_data = fetch_itemwise(from_date_obj, to_date_obj)
#             previous_data = fetch_itemwise(prev_from, prev_to)

#             # ---- merge current & previous ----
#             items = []
#             all_item_ids = set(current_data) | set(previous_data)

#             for item_id in all_item_ids:
#                 items.append({
#                     "item_id": item_id,
#                     "item_name": (
#                         current_data.get(item_id)
#                         or previous_data.get(item_id)
#                     )["item_name"],
#                     "current_quantity": current_data.get(item_id, {}).get("quantity", 0),
#                     "previous_quantity": previous_data.get(item_id, {}).get("quantity", 0),
#                 })

#             return {
#                 "dataview": dataview,
#                 "items": sorted(items, key=lambda x: x["item_name"]),
#             }

#     except Exception as e:
#         print("ITEMWISE COMPARISON ERROR:", e)
#         raise HTTPException(status_code=500, detail=str(e))




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