import io
import xlsxwriter
from fastapi.responses import StreamingResponse
from fastapi import APIRouter, HTTPException
from app.database import engine
from app.primary_order.schemas.po_schemas import OrderSummaryFilters
from app.primary_order.utils.po_export_helper import (
    build_filters,
    build_order_summary_query
)

router = APIRouter()

EXPORT_COLUMN_MAP = {
    "order_id": "Order No.",
    "order_code": "Order Code",
    "order_sap_id": "SAP Number",
    "total": "Total Amount",
    "warehouse_name": "Distributor Name",
    "delivery_sap_id": "Delivery Number",
    "invoice_sap_id": "Invoice Number",
    "unique_item_count": "Total Items"
}


@router.post("/po-order-export")
def export_order_summary_xlsx(filters: OrderSummaryFilters):

    where_sql, params = build_filters(filters)
    query = build_order_summary_query(where_sql)

    try:
        with engine.connect() as conn:
            result = conn.execute(query, params)
            rows = result.fetchall()
            db_columns = result.keys()   # original SQL column names

        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {"in_memory": True})
        worksheet = workbook.add_worksheet("Order Summary")

        header_format = workbook.add_format({
            "bold": True,
            "border": 1,
            "align": "center"
        })

        cell_format = workbook.add_format({
            "border": 1
        })

        # 🔹 Write renamed headers
        for col_idx, col_key in enumerate(db_columns):
            header_name = EXPORT_COLUMN_MAP.get(col_key, col_key)
            worksheet.write(0, col_idx, header_name, header_format)
            worksheet.set_column(col_idx, col_idx, 22)

        # 🔹 Write data rows (unchanged)
        for row_idx, row in enumerate(rows, start=1):
            for col_idx, value in enumerate(row):
                worksheet.write(row_idx, col_idx, value, cell_format)

        workbook.close()
        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=order_summary.xlsx"
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
