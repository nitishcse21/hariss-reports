# from fastapi import APIRouter, Query, HTTPException
# from typing import Optional
# from app.attendance_report.utils.common_helper import parse_csv_ids
# from app.database import engine
# from sqlalchemy import text


# router = APIRouter()


# @router.get("/attendance-filter")
# def attendance_filter(
#     warehouse_ids: Optional[str] = Query(None),
#     salesman_ids: Optional[str] = Query(None),
# ):

#     warehouse_ids_list = parse_csv_ids(warehouse_ids)
#     salesman_ids_list = parse_csv_ids(salesman_ids)

#     out = {}

#     try:
#         with engine.connect() as conn:
#             q = "SELECT id, warehouse_name FROM tbl_warehouse ORDER BY warehouse_name"
#             out["warehouse"] = [
#                 dict(r._mapping) for r in conn.execute(text(q)).fetchall()
#             ]

#             if warehouse_ids_list:
#                 pg_array = ",".join(str(x) for x in warehouse_ids_list)
#                 q = """
#                     SELECT  id, osa_code || '-' || name as salesman_name
#                     FROM salesman
#                     WHERE string_to_array(warehouse_id, ',') && string_to_array(:warehouse_ids, ',')
#                     ORDER BY osa_code
#                 """
#                 out["salesman"] = [
#                     dict(r._mapping)
#                     for r in conn.execute(
#                         text(q), {"warehouse_ids":pg_array},
#                     ).fetchall()
#                 ]

#             else:
#                 q = """
#                     SELECT id, osa_code || '-' || name as salesman_name
#                     FROM salesman
#                     ORDER BY osa_code
#                 """
#                 out["salesman"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]


#     except Exception as e:
#         print("FILTER ERROR:", e)
#         raise HTTPException(status_code=500, detail=str(e))
    
#     return out


from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from app.attendance_report.utils.common_helper import parse_csv_ids
from app.database import engine
from sqlalchemy import text

router = APIRouter()




@router.get("/attendance-filter")
def attendance_filter(
    salesman_type_name: str = Query("Projects"),
    warehouse_ids: Optional[str] = Query(None),
    salesman_ids: Optional[str] = Query(None),
):

    warehouse_ids_list = parse_csv_ids(warehouse_ids)
    salesman_ids_list = parse_csv_ids(salesman_ids)

    out = {}

    try:
        with engine.connect() as conn:

            # ----------------------------
            # WAREHOUSE FILTERED BY TYPE
            # ----------------------------
            q = """
                SELECT DISTINCT
                    w.id,
                    w.warehouse_name
                FROM salesman s
                JOIN salesman_types st ON st.id = s.type
                JOIN tbl_warehouse w
                  ON w.id::text = ANY(string_to_array(s.warehouse_id, ','))
                WHERE st.salesman_type_name = :salesman_type_name
                ORDER BY w.warehouse_name
            """
            out["warehouse"] = [
                dict(r._mapping)
                for r in conn.execute(
                    text(q),
                    {"salesman_type_name": salesman_type_name},
                ).fetchall()
            ]

            # ----------------------------
            # SALESMAN FILTERED BY TYPE
            # ----------------------------
            if warehouse_ids_list:
                pg_array = ",".join(str(x) for x in warehouse_ids_list)

                q = """
                    SELECT
                        s.id,
                        s.osa_code || '-' || s.name AS salesman_name
                    FROM salesman s
                    JOIN salesman_types st ON st.id = s.type
                    WHERE st.salesman_type_name = :salesman_type_name
                      AND string_to_array(s.warehouse_id, ',')
                          && string_to_array(:warehouse_ids, ',')
                    ORDER BY s.osa_code
                """

                out["salesman"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q),
                        {
                            "salesman_type_name": salesman_type_name,
                            "warehouse_ids": pg_array,
                        },
                    ).fetchall()
                ]

            else:
                q = """
                    SELECT
                        s.id,
                        s.osa_code || '-' || s.name AS salesman_name
                    FROM salesman s
                    JOIN salesman_types st ON st.id = s.type
                    WHERE st.salesman_type_name = :salesman_type_name
                    ORDER BY s.osa_code
                """

                out["salesman"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q),
                        {"salesman_type_name": salesman_type_name},
                    ).fetchall()
                ]

    except Exception as e:
        print("FILTER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

    return out
