import io
import xlsxwriter
from sqlalchemy import text
from fastapi import APIRouter
from app.database import engine
from fastapi.responses import StreamingResponse
from app.visit_report.schemas.visit_schema import VisitSchema
from app.visit_report.utils.visit_common_helper import validate_mandatory, build_query_parts

router = APIRouter()




@router.post("/visit-export")
def visit_export_xlsx(filters: VisitSchema):
    validate_mandatory(filters)
    joins, where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)
    join_sql_base = "\n".join(joins)

    with engine.connect() as conn:

        base_sql = f"""
            FROM visit_plan vp
            JOIN salesman s ON vp.salesman_id = s.id
            JOIN salesman_types st ON s.type = st.id
            JOIN agent_customers ac ON vp.customer_id = ac.id
            JOIN tbl_warehouse w ON vp.warehouse_id = w.id
            {join_sql_base}
            WHERE
                {where_sql}
        """

        sql = f"""
            SELECT
                ac.osa_code || '-' || ac.name AS "Customer Name",
                ac.contact_no AS "Customer Contact",
                ac.district AS "Customer District",
                s.osa_code || '-' || s.name AS "Salesman Name",
                st.salesman_type_name AS "Salesman Role",
                w.warehouse_code || '-' || w.warehouse_name AS "Depot Name",
                TO_CHAR(vp.visit_start_time, 'YYYY-MM-DD') AS "Visit Start Date",
                TO_CHAR(vp.visit_end_time, 'YYYY-MM-DD') AS "Visit End Date",
                TO_CHAR(vp.visit_start_time, 'HH24:MI:SS') AS "Visit Start Time",
                TO_CHAR(vp.visit_end_time, 'HH24:MI:SS') AS "Visit End Time",
                CASE 
                    WHEN vp.shop_status = '1' THEN 'Open'
                    WHEN vp.shop_status = '0' THEN 'Closed'
                    ELSE 'Unknown'
                END AS "Shop Status",


                -- Distance in meters (Haversine)
                ROUND(
                    6371000 * acos(
                        cos(radians(ac.latitude)) *
                        cos(radians(vp.latitude)) *
                        cos(radians(vp.longitude) - radians(ac.longitude)) +
                        sin(radians(ac.latitude)) *
                        sin(radians(vp.latitude))
                    )
                ) AS "Distance (Meters)"

            {base_sql}
            ORDER BY vp.created_at DESC
        """

        result = conn.execute(text(sql), params)
        rows = [dict(r._mapping) for r in result]

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output)
    sheet = workbook.add_worksheet("Visit Report")

    headers = rows[0].keys() if rows else []
    for col, header in enumerate(headers):
        sheet.write(0, col, header)

    for row_idx, row in enumerate(rows, start=1):
        for col_idx, value in enumerate(row.values()):
            sheet.write(row_idx, col_idx, value)

    workbook.close()
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=visit_report.xlsx"}
    )




