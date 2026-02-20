from fastapi import APIRouter, Request, Query
from sqlalchemy import text
from app.fridge_tracking_report.schemas.fridge_schema import FridgeTrackingRequest
from app.fridge_tracking_report.utils.fridge_helper import (
    validate_mandatory,
    build_query_parts,
)
from app.database import engine

router = APIRouter()


@router.post("/fridge-table")
def fridge_table(
    filters: FridgeTrackingRequest, request: Request, page: int = Query(1, ge=1)
):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)
    ROW_PER_PAGE = 50

    with engine.connect() as conn:
        base_sql = f"""FROM tbl_fridge_tracking_report ft
                JOIN tbl_route rt ON rt.id = ft.route_id
                JOIN agent_customers ac ON ac.id = ft.customer_id
                JOIN salesman s ON s.id = ft.salesman_id
                LEFT JOIN tbl_add_chillers tac ON tac.customer_id = ac.id
                JOIN tbl_warehouse w ON w.id = ac.warehouse
                JOIN tbl_region r ON r.id = w.region_id
                JOIN tbl_areas a ON a.id = w.area_id                
                WHERE {where_sql}"""

        count_sql = f"SELECT COUNT(*) {base_sql}"
        total_rows = conn.execute(text(count_sql), params).scalar()
        offset = (page - 1) * ROW_PER_PAGE
        params["limit"] = ROW_PER_PAGE
        params["offset"] = offset

        query = f"""
                SELECT 
                TO_CHAR( ft.created_at, 'YYYY-MM-DD') AS Date,
                ft.outlet_name,
                rt.route_name AS route_name,
                w.warehouse_code || '-' || w.warehouse_name AS warehouse_name,
                a.area_code || '-' || a.area_name AS area_name,
                r.region_code || '-' || r.region_name AS region_name,
                s.name AS salesman_name,
                ft.have_fridge,
                tac.serial_number AS assign_serial_number,
                ft.serial_no AS captured_serial_number,
                ft.complaint_type AS complaint_type,
                ft.latitude,
                ft.longitude,
                ft.image,
                ft.comments
                {base_sql}
                ORDER BY ft.created_at
                LIMIT :limit OFFSET :offset
            """
        
        rows = conn.execute(text(query), params).fetchall()
        # rows_data = [dict(r._mapping) for r in rows]
        rows_data = []
        total_pages = (total_rows + ROW_PER_PAGE - 1) // ROW_PER_PAGE
        base_url = str(request.url).split("?")[0]
        BASE_URL = "http://osa.harissint.com/upload_image/fridge_tracking_report/"
        
        for r in rows:
            row = dict(r._mapping)
            if row.get("image"):
                images = row["image"].split(",")
                row["image"] = [
                    BASE_URL + img.strip()
                    for img in images if img.strip()
                ]
            else:
                row["image"] = []
            rows_data.append(row)
        return {
            "total_rows": total_rows,
            "total_pages": total_pages,
            "current_page": page,
            "next_page": f"{base_url}?page={page + 1}" if page < total_pages else None,
            "previous_page": f"{base_url}?page={page - 1}" if page > 1 else None,
            "rows": rows_data,
        }
