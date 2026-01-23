
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text
import pandas as pd
import io

from database import engine
from customer_report.utils.customer_sales_download_helper import (
    build_where,
    DEFAULT_SQL,
    DETAIL_SQL
)
from customer_report.schemas.customer_sales_schema import DownloadRequest

app = APIRouter()


@app.post("/customer-sales-download")
def download_customer_sales(payload: DownloadRequest):

    # ---------------- VALIDATION ----------------
    if payload.search_type not in ("quantity", "amount"):
        raise HTTPException(400, "search_type must be quantity or amount")

    if payload.view_type not in ("default", "detail"):
        raise HTTPException(400, "view_type must be default or detail")

    if payload.file_type not in ("csv", "xlsx"):
        raise HTTPException(400, "file_type must be csv or xlsx")

    if payload.display_quantity not in ("with_free_good", "without_free_good"):
        raise HTTPException(400, "display_quantity invalid")

    # ---------------- VALUE EXPRESSION (PER ROW) ----------------
    if payload.search_type == "quantity":
        value_expr = """
            CASE
                WHEN ms.uom IN (1,3)
                     AND NULLIF(iu.upc,0) IS NOT NULL
                THEN ms.total_quantity / iu.upc
                ELSE ms.total_quantity
            END
        """
        value_label = "Total Quantity"
    else:
        value_expr = "ms.total_amount"
        value_label = "Total Amount"

    # ---------------- WHERE CLAUSE ----------------
    where_sql, params = build_where(payload)

    # ---------------- SQL PICK ----------------
    sql_template = DEFAULT_SQL if payload.view_type == "default" else DETAIL_SQL

    sql = sql_template.format(
        value_expr=value_expr,
        value_label=value_label,
        where_sql=where_sql
    )

    # ---------------- EXECUTE ----------------
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    # ---------------- EXPORT ----------------
    if payload.file_type == "csv":
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        return StreamingResponse(
            buffer,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=customer_sales_report.csv"}
        )

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Customer Sales")

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=customer_sales_report.xlsx"}
    )

