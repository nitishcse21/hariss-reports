
from fastapi import APIRouter, Query, Request, HTTPException
from sqlalchemy import text
from app.database import engine
from app.load_unload_report.schemas.load_unload_schema import LoadUnloadReportRequest

router = APIRouter()

@router.post("/load-unload-table")
def load_unload_report(
    request: Request,
    payload: LoadUnloadReportRequest,
    page: int = Query(1)
):

    # -------- HARD VALIDATION --------
    if payload.warehouse_id is None and payload.salesman_id is None:
        raise HTTPException(
            status_code=400,
            detail="Please select either warehouse or salesman"
        )

    limit = 50
    offset = (page - 1) * limit

    base_sql = """
    FROM items i

    LEFT JOIN (
        SELECT 
            d.item_id,
            ROUND(SUM(
                CASE 
                    WHEN d.uom IN (1,3) 
                    THEN d.qty / NULLIF(iu.upc::numeric, 0)
                    ELSE d.qty
                END
            )::numeric, 3) AS load_qty
        FROM tbl_load_header h
        JOIN tbl_load_details d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        WHERE h.created_at BETWEEN :from_date AND :to_date
          AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
          AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY d.item_id
    ) l ON l.item_id = i.id

    LEFT JOIN (
        SELECT 
            d.item_id,
            ROUND(SUM(
                CASE 
                    WHEN d.uom IN (1,3) 
                    THEN d.qty / NULLIF(iu.upc::numeric, 0)
                    ELSE d.qty
                END
            )::numeric, 3) AS unload_qty
        FROM tbl_unload_header h
        JOIN tbl_unload_detail d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        WHERE h.unload_date BETWEEN :from_date AND :to_date
          AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
          AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY d.item_id
    ) u ON u.item_id = i.id

    LEFT JOIN (
        SELECT 
            d.item_id,
            ROUND(SUM(
                CASE 
                    WHEN d.uom IN (1,3) 
                    THEN d.quantity / NULLIF(iu.upc::numeric, 0)
                    ELSE d.quantity
                END
            )::numeric, 3) AS sales_qty
        FROM invoice_headers h
        JOIN invoice_details d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        WHERE h.invoice_date BETWEEN :from_date AND :to_date
          AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
          AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY d.item_id
    ) s ON s.item_id = i.id

    WHERE 
        COALESCE(l.load_qty, 0) <> 0
     OR COALESCE(u.unload_qty, 0) <> 0
     OR COALESCE(s.sales_qty, 0) <> 0
    """

    count_sql = text("SELECT COUNT(*) " + base_sql)

    data_sql = text(f"""
        SELECT
            i.code || '-' || i.name AS item_name,
            COALESCE(l.load_qty, 0)   AS load_quantity,
            COALESCE(u.unload_qty, 0) AS unload_quantity,
            COALESCE(s.sales_qty, 0)  AS sales_quantity
        {base_sql}
        ORDER BY i.code
        LIMIT :limit OFFSET :offset
    """)

    params = payload.dict()
    params["limit"] = limit
    params["offset"] = offset

    with engine.connect() as conn:
        total_rows = conn.execute(count_sql, params).scalar()
        rows = conn.execute(data_sql, params).mappings().all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No data found for selected filters"
        )

    total_pages = (total_rows + limit - 1) // limit

    base_url = str(request.url).split("?")[0]

    next_page = (
        f"{base_url}?page={page+1}"
        if page < total_pages else None
    )

    previous_page = (
        f"{base_url}?page={page-1}"
        if page > 1 else None
    )

    return {
        "total_rows": total_rows,
        "total_pages": total_pages,
        "current_page": page,
        "next_page": next_page,
        "previous_page": previous_page,
        "rows": rows
    }
