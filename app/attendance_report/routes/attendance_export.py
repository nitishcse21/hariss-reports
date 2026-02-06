# from fastapi import APIRouter, HTTPException, Response
# from sqlalchemy import text
# from app.database import engine
# from app.attendance_report.schemas.attendance_schema import AttendanceExportRequest
# import io
# import xlsxwriter

# router = APIRouter()

# @router.post("/attendance-export-xlsx")
# def attendance_export_xlsx(filters: AttendanceExportRequest):

#     if not filters.warehouse_ids and not filters.salesman_ids:
#         raise HTTPException(status_code=400, detail="Select warehouse or salesman")

#     try:
#         with engine.connect() as conn:

#             # ---------------------------
#             # CASE 1: SINGLE SALESMAN
#             # ---------------------------
#             if filters.salesman_ids and len(filters.salesman_ids) == 1:
#                 sid = filters.salesman_ids[0]

#                 q = """
#                     SELECT
#                         s.osa_code || '-' || s.name AS salesman_name,
#                         sa.attendance_date::date AS date,
#                         TO_CHAR(sa.time_in, 'HH24:MI:SS') AS in_time,
#                         TO_CHAR(sa.time_out, 'HH24:MI:SS') AS out_time
#                     FROM salesman_attendance sa
#                     JOIN salesman s ON s.id = sa.salesman_id
#                     WHERE sa.salesman_id = :sid
#                       AND sa.attendance_date BETWEEN :from_date AND :to_date
#                     ORDER BY sa.attendance_date
#                 """
#                 rows = conn.execute(
#                     text(q),
#                     {
#                         "sid": sid,
#                         "from_date": filters.from_date,
#                         "to_date": filters.to_date,
#                     },
#                 ).fetchall()
#                 mode = "daily"

#             # ---------------------------
#             # CASE 2: SUMMARY MODE
#             # ---------------------------
#             else:
#                 base_sql = """
#                     SELECT
#                         s.osa_code || '-' || s.name AS salesman_name,
#                         w.warehouse_name,
#                         COUNT(DISTINCT sa.attendance_date) AS total_working_days
#                     FROM salesman s
#                     JOIN tbl_warehouse w
#                       ON w.id::text = ANY(string_to_array(s.warehouse_id, ','))
#                     LEFT JOIN salesman_attendance sa
#                       ON sa.salesman_id = s.id
#                      AND sa.time_in IS NOT NULL
#                      AND sa.attendance_date BETWEEN :from_date AND :to_date
#                 """

#                 where = []
#                 params = {
#                     "from_date": filters.from_date,
#                     "to_date": filters.to_date,
#                 }

#                 if filters.salesman_ids:
#                     where.append("s.id = ANY(:salesman_ids)")
#                     params["salesman_ids"] = filters.salesman_ids

#                 elif filters.warehouse_ids:
#                     where.append(
#                         "string_to_array(s.warehouse_id, ',') && string_to_array(:warehouse_ids, ',')"
#                     )
#                     params["warehouse_ids"] = ",".join(map(str, filters.warehouse_ids))

#                 if where:
#                     base_sql += " WHERE " + " AND ".join(where)

#                 base_sql += """
#                     GROUP BY s.osa_code, s.name, w.warehouse_name
#                     ORDER BY s.osa_code
#                 """

#                 rows = conn.execute(text(base_sql), params).fetchall()
#                 mode = "summary"

#         # ---------------------------
#         # CREATE XLSX
#         # ---------------------------
#         output = io.BytesIO()
#         workbook = xlsxwriter.Workbook(output, {"in_memory": True})
#         sheet = workbook.add_worksheet("Attendance")

#         header = workbook.add_format({"bold": True})

#         if mode == "daily":
#             headers = ["Salesman", "Date", "In Time", "Out Time"]
#             for c, h in enumerate(headers):
#                 sheet.write(0, c, h, header)

#             for r, row in enumerate(rows, start=1):
#                 m = dict(row._mapping)
#                 sheet.write(r, 0, m["salesman_name"])
#                 sheet.write(r, 1, str(m["date"]))
#                 sheet.write(r, 2, m["in_time"])
#                 sheet.write(r, 3, m["out_time"])

#         else:
#             headers = ["S.No", "Salesman", "Warehouse", "Total Working Days"]
#             for c, h in enumerate(headers):
#                 sheet.write(0, c, h, header)

#             for i, row in enumerate(rows, start=1):
#                 m = dict(row._mapping)
#                 sheet.write(i, 0, i)
#                 sheet.write(i, 1, m["salesman_name"])
#                 sheet.write(i, 2, m["warehouse_name"])
#                 sheet.write(i, 3, m["total_working_days"])

#         workbook.close()
#         output.seek(0)

#         return Response(
#             output.read(),
#             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#             headers={"Content-Disposition": "attachment; filename=attendance.xlsx"},
#         )

#     except Exception as e:
#         print("EXPORT ERROR:", e)
#         raise HTTPException(status_code=500, detail=str(e))




































from fastapi import APIRouter, HTTPException, Response
from sqlalchemy import text
from app.database import engine
from app.attendance_report.schemas.attendance_schema import AttendanceRequest
import io
import xlsxwriter

router = APIRouter()

@router.post("/attendance-export-xlsx")
def attendance_export_xlsx(filters: AttendanceRequest):

    if not filters.warehouse_ids and not filters.salesman_ids:
        raise HTTPException(status_code=400, detail="Select warehouse or salesman")

    try:
        with engine.connect() as conn:

            # ---------------------------
            # CASE 1: SINGLE SALESMAN
            # ---------------------------
            if filters.salesman_ids and len(filters.salesman_ids) == 1:
                sid = filters.salesman_ids[0]

                q = """
                    SELECT
                        s.osa_code || '-' || s.name AS salesman_name,
                        st.salesman_type_name,
                        sa.attendance_date::date AS date,
                        TO_CHAR(sa.time_in, 'HH24:MI:SS') AS in_time,
                        TO_CHAR(sa.time_out, 'HH24:MI:SS') AS out_time
                    FROM salesman_attendance sa
                    JOIN salesman s ON s.id = sa.salesman_id
                    JOIN salesman_types st ON st.id = s.type
                    WHERE sa.salesman_id = :sid
                    AND st.salesman_type_name = :search_type
                    AND sa.attendance_date BETWEEN :from_date AND :to_date
                    ORDER BY sa.attendance_date
                """

                rows = conn.execute(
                    text(q),
                    {
                        "sid": sid,
                        "search_type": filters.search_type,
                        "from_date": filters.from_date,
                        "to_date": filters.to_date,
                    },
                ).fetchall()
                mode = "daily"

            # ---------------------------
            # CASE 2: SUMMARY MODE
            # ---------------------------
            else:
                base_sql = """
                    SELECT
                        s.osa_code || '-' || s.name AS salesman_name,
                        st.salesman_type_name,
                        w.warehouse_name,
                        COUNT(DISTINCT sa.attendance_date) AS total_working_days
                    FROM salesman s
                    JOIN salesman_types st ON st.id = s.type
                    JOIN tbl_warehouse w
                    ON w.id::text = ANY(string_to_array(s.warehouse_id, ','))
                    LEFT JOIN salesman_attendance sa
                    ON sa.salesman_id = s.id
                    AND sa.time_in IS NOT NULL
                    AND sa.attendance_date BETWEEN :from_date AND :to_date
                """



                where = []
                params = {
                    "from_date": filters.from_date,
                    "to_date": filters.to_date,
                    "search_type": filters.search_type,
                }

                # mandatory filter
                where.append("st.salesman_type_name = :search_type")

                if filters.salesman_ids:
                    where.append("s.id = ANY(:salesman_ids)")
                    params["salesman_ids"] = filters.salesman_ids

                elif filters.warehouse_ids:
                    where.append(
                        "string_to_array(s.warehouse_id, ',') && string_to_array(:warehouse_ids, ',')"
                    )
                    params["warehouse_ids"] = ",".join(map(str, filters.warehouse_ids))

                if where:
                    base_sql += " WHERE " + " AND ".join(where)

                base_sql += """
                    GROUP BY
                        s.osa_code,
                        s.name,
                        st.salesman_type_name,
                        w.warehouse_name
                    ORDER BY s.osa_code
                """

                rows = conn.execute(text(base_sql), params).fetchall()
                mode = "summary"

        # ---------------------------
        # CREATE XLSX
        # ---------------------------
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {"in_memory": True})
        sheet = workbook.add_worksheet("Attendance")

        header = workbook.add_format({"bold": True})

        if mode == "daily":
            headers = ["Salesman", "Salesman Type", "Date", "In Time", "Out Time"]
            for c, h in enumerate(headers):
                sheet.write(0, c, h, header)

            for r, row in enumerate(rows, start=1):
                m = dict(row._mapping)
                sheet.write(r, 0, m["salesman_name"])
                sheet.write(r, 1, m["salesman_type_name"])
                sheet.write(r, 2, str(m["date"]))
                sheet.write(r, 3, m["in_time"])
                sheet.write(r, 4, m["out_time"])

        else:
            headers = ["S.No", "Salesman", "Salesman Type", "Warehouse", "Total Working Days"]
            for c, h in enumerate(headers):
                sheet.write(0, c, h, header)

            for i, row in enumerate(rows, start=1):
                m = dict(row._mapping)
                sheet.write(i, 0, i)
                sheet.write(i, 1, m["salesman_name"])
                sheet.write(i, 2, m["salesman_type_name"])
                sheet.write(i, 3, m["warehouse_name"])
                sheet.write(i, 4, m["total_working_days"])


        workbook.close()
        output.seek(0)

        return Response(
            output.read(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=attendance.xlsx"},
        )

    except Exception as e:
        print("EXPORT ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))
