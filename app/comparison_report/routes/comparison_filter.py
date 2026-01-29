from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from app.comparison_report.utils.comparison_common_helper import parse_csv_ids
from app.database import engine
from sqlalchemy import text


router = APIRouter()


@router.get("/comparison-filter")
def comparison_filter(
    warehouse_ids: Optional[str] = Query(None),
    salesman_ids: Optional[str] = Query(None),
):

    warehouse_ids_list = parse_csv_ids(warehouse_ids)
    salesman_ids_list = parse_csv_ids(salesman_ids)

    out = {}

    try:
        with engine.connect() as conn:
            q = "SELECT id, warehouse_name FROM tbl_warehouse ORDER BY warehouse_name"
            out["warehouse"] = [
                dict(r._mapping) for r in conn.execute(text(q)).fetchall()
            ]

            if warehouse_ids_list:
                pg_array = ",".join(str(x) for x in warehouse_ids_list)
                q = """
                    SELECT  id, osa_code || '-' || name as salesman_name
                    FROM salesman
                    WHERE string_to_array(warehouse_id, ',') && string_to_array(:warehouse_ids, ',')
                    ORDER BY osa_code
                """
                out["salesman"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q), {"warehouse_ids":pg_array},
                    ).fetchall()
                ]

            else:
                q = """
                    SELECT id, osa_code || '-' || name as salesman_name
                    FROM salesman
                    ORDER BY osa_code
                """
                out["salesman"] = [dict(r._mapping) for r in conn.execute(text(q)).fetchall()]


    except Exception as e:
        print("FILTER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))
    
    return out