# from fastapi import APIRouter
# from fastapi.responses import StreamingResponse
# from sqlalchemy import text
# from app.database import engine
# from app.load_unload_report.schemas.load_unload_schema import LoadUnloadReportRequest
# import openpyxl
# from io import BytesIO

# router = APIRouter()

# @router.post("/load-unload-report-export")
# def load_unload_report_export(payload: LoadUnloadReportRequest):

#     sql = text("""
#     SELECT
#         i.code || '-' || i.name AS item_name,
#         COALESCE(l.load_qty, 0)   AS load_quantity,
#         COALESCE(u.unload_qty, 0) AS unload_quantity,
#         COALESCE(s.sales_qty, 0)  AS sales_quantity
#     FROM items i

#     LEFT JOIN (
#         SELECT 
#             d.item_id,
#             SUM(
#                 CASE 
#                     WHEN d.uom IN (1,3) THEN d.qty / NULLIF(iu.upc::numeric, 0)
#                     ELSE d.qty
#                 END
#             ) AS load_qty
#         FROM tbl_load_header h
#         JOIN tbl_load_details d ON h.id = d.header_id
#         LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
#         WHERE h.created_at BETWEEN :from_date AND :to_date
#           AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
#           AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
#         GROUP BY d.item_id
#     ) l ON l.item_id = i.id

#     LEFT JOIN (
#         SELECT 
#             d.item_id,
#             SUM(
#                 CASE 
#                     WHEN d.uom IN (1,3) THEN d.qty / NULLIF(iu.upc::numeric, 0)
#                     ELSE d.qty
#                 END
#             ) AS unload_qty
#         FROM tbl_unload_header h
#         JOIN tbl_unload_detail d ON h.id = d.header_id
#         LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
#         WHERE h.unload_date BETWEEN :from_date AND :to_date
#           AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
#           AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
#         GROUP BY d.item_id
#     ) u ON u.item_id = i.id

#     LEFT JOIN (
#         SELECT 
#             d.item_id,
#             SUM(
#                 CASE 
#                     WHEN d.uom IN (1,3) THEN d.quantity / NULLIF(iu.upc::numeric, 0)
#                     ELSE d.quantity
#                 END
#             ) AS sales_qty
#         FROM invoice_headers h
#         JOIN invoice_details d ON h.id = d.header_id
#         LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
#         WHERE h.invoice_date BETWEEN :from_date AND :to_date
#           AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
#           AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
#         GROUP BY d.item_id
#     ) s ON s.item_id = i.id
#     WHERE 
#     COALESCE(l.load_qty, 0) <> 0
#     OR COALESCE(u.unload_qty, 0) <> 0
#     OR COALESCE(s.sales_qty, 0) <> 0
#     ORDER BY i.code
#     """)

#     with engine.connect() as conn:
#         result = conn.execute(sql, payload.dict())
#         rows = result.mappings().all()

#     # ---------------- Excel Generation ----------------

#     wb = openpyxl.Workbook()
#     ws = wb.active
#     ws.title = "Load Unload Report"

#     headers = ["Item Name", "Load Quantity", "Unload Quantity", "Sales Quantity"]
#     ws.append(headers)

#     for row in rows:
#         ws.append([
#             row["item_name"],
#             row["load_quantity"],
#             row["unload_quantity"],
#             row["sales_quantity"]
#         ])

#     stream = BytesIO()
#     wb.save(stream)
#     stream.seek(0)

#     return StreamingResponse(
#         stream,
#         media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#         headers={
#             "Content-Disposition": "attachment; filename=load_unload_report.xlsx"
#         }
#     )




from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from app.database import engine
from app.load_unload_report.schemas.load_unload_schema import LoadUnloadReportRequest
import openpyxl
from io import BytesIO

router = APIRouter()

@router.post("/load-unload-export")
def load_unload_report_export(payload: LoadUnloadReportRequest):

    # ----------- HARD VALIDATION -----------
    if payload.warehouse_id is None and payload.salesman_id is None:
        raise HTTPException(
            status_code=400,
            detail="Please select either warehouse or salesman"
        )

    sql = text("""
    SELECT
        i.code || '-' || i.name AS item_name,
        COALESCE(l.load_qty, 0)   AS load_quantity,
        COALESCE(u.unload_qty, 0) AS unload_quantity,
        COALESCE(s.sales_qty, 0)  AS sales_quantity
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

    ORDER BY i.code
    """)

    with engine.connect() as conn:
        result = conn.execute(sql, payload.dict())
        rows = result.mappings().all()

    # ----------- NO DATA SAFETY -----------
    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No data found for selected filters"
        )

    # ----------- EXCEL GENERATION -----------

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Load Unload Report"

    headers = ["Item Name", "Load Quantity", "Unload Quantity", "Sales Quantity"]
    ws.append(headers)

    for row in rows:
        ws.append([
            row["item_name"],
            float(row["load_quantity"]),
            float(row["unload_quantity"]),
            float(row["sales_quantity"])
        ])

    stream = BytesIO()
    wb.save(stream)
    stream.seek(0)

    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=load_unload_report.xlsx"
        }
    )
