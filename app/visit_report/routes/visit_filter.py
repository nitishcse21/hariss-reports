from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from app.visit_report.utils.visit_common_helper import parse_csv_ids
from app.database import engine
from sqlalchemy import text


router = APIRouter()


@router.get("/visit-filter")
def visit_filter(
    warehouse_ids: Optional[str] = Query(None),
    route_ids: Optional[str] = Query(None),
    salesman_ids: Optional[str] = Query(None),
):
    warehouse_ids_list = parse_csv_ids(warehouse_ids)
    route_ids_list = parse_csv_ids(route_ids)
    salesman_ids_list = parse_csv_ids(salesman_ids)

    out = {}

    try:
        with engine.connect() as conn:
            q = "SELECT id, warehouse_code || ' - ' || warehouse_name AS label FROM tbl_warehouse ORDER BY warehouse_name"
            out["warehouse"] = [
                dict(r._mapping) for r in conn.execute(text(q)).fetchall()
            ]

            if warehouse_ids_list:
                q = """ 
                    SELECT id, route_code || ' - ' || route_name AS label
                    FROM tbl_route
                    WHERE warehouse_id IN :warehouse_ids
                    ORDER BY route_code
                    """
                out["routes"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q), {"warehouse_ids": tuple(warehouse_ids_list)}
                    ).fetchall()
                ]

            else:
                q = """
                    SELECT id, route_code || '-' || route_name as label
                    FROM tbl_route
                    ORDER BY route_code
                """
                out["routes"] = [
                    dict(r._mapping) for r in conn.execute(text(q)).fetchall()
                ]
            
            if route_ids_list:
                q = f"""
                    SELECT id, osa_code || ' - ' || name AS label
                    FROM salesman
                    WHERE route_id IN :route_ids
                    ORDER BY osa_code
                    """
                out["salesman"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q), {"route_ids": tuple(route_ids_list)}
                    ).fetchall()
                ]

            elif warehouse_ids_list:
                pg_array = ",".join(str(x) for x in warehouse_ids_list)
                q = f"""
                    SELECT id, osa_code || ' - ' || name AS label
                    from salesman
                    WHERE string_to_array(warehouse_id, ',') && string_to_array(:warehouse_ids, ',')
                    ORDER BY osa_code
                    """
                
                out["salesman"] = [
                    dict(r._mapping)
                    for r in conn.execute(
                        text(q), {"warehouse_ids": pg_array},
                    ).fetchall()
                ]
            else:
                q = """
                    SELECT id, osa_code || ' - ' || name AS label
                    FROM salesman
                    ORDER BY osa_code
                    """
                out["salesman"] = [
                    dict(r._mapping) for r in conn.execute(text(q)).fetchall()
                ]

    except Exception as e:
        print("FILTER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

    return out