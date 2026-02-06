from fastapi import APIRouter, Query, Request
from app.attendance_report.schemas.attendance_schema import AttendanceRequest
from app.attendance_report.utils.common_helper import (
    validate_mandatory,
    build_query_parts,
)
from app.database import engine
from sqlalchemy import text

router = APIRouter()


@router.post("/attendance-table")
def attendance_table(
    filters: AttendanceRequest, request: Request, page: int = Query(1, ge=1)
):

    page_size = 50
    validate_mandatory(filters)

    joins, where_fragments, params = build_query_parts(filters)
    join_sql = "\n".join(joins)
    where_sql = " AND ".join(where_fragments)


    salesman_type = f"""
        SELECT id FROM salesman_types WHERE LOWER(salesman_type_name) = :search_type
    """
    with engine.connect() as conn:
        salsman_type_id = conn.execute(
            text(salesman_type), {"search_type": filters.search_type.lower()}
        ).scalar()

        salsman_type_id = f"s.type = {salsman_type_id}"
    
   
    # if filters.search_type.lower() == "projects":
    #     salsman_type_id = "s.type = 6"
    # elif filters.search_type.lower() == "salesman":
    #     salsman_type_id = "s.type = 3"
    # else:
    #     salsman_type_id = "s.type = 2"

    base_sql = f"""
        FROM salesman_attendance AS sa
        JOIN tbl_warehouse w ON w.id = sa.warehouse_id
        {join_sql}
        JOIN salesman s ON s.id = sa.salesman_id
        JOIN salesman_types st ON st.id = s.type
        WHERE {where_sql} AND {salsman_type_id}
    """

    
    count_sql = f"SELECT COUNT(*) {base_sql}"

    with engine.connect() as conn:
        total_rows = conn.execute(text(count_sql), params).scalar()

    offset = (page - 1) * page_size
    params["limit"] = page_size
    params["offset"] = offset

    data_sql = f"""
        SELECT 
            w.warehouse_name,
            s.osa_code || '-' || s.name AS salesman_name,
            TO_CHAR(sa.time_in, 'HH24:MI:SS') AS time_in,
            TO_CHAR(sa.time_out, 'HH24:MI:SS') AS time_out,
            '​https://api.coreexl.com/osa_developmentV2/public/storage/' || sa.in_img AS in_img,
            'https://api.coreexl.com/osa_developmentV2/public/storage/' || sa.out_img AS out_img,
            st.salesman_type_name
        {base_sql}
        GROUP BY 
            w.warehouse_name, s.osa_code, s.name,
            sa.time_in, sa.time_out,
            st.salesman_type_name, sa.in_img, sa.out_img
        ORDER BY sa.time_in DESC
        LIMIT :limit OFFSET :offset
    """

    with engine.connect() as conn:
        rows = conn.execute(text(data_sql), params).fetchall()

    rows_data = [dict(r._mapping) for r in rows]
    total_pages = (total_rows + page_size - 1) // page_size
    base_url = str(request.url).split("?")[0]

    return {
        "total_rows": total_rows,
        "total_pages": total_pages,
        "current_page": page,
        "next_page": f"{base_url}?page={page + 1}" if page < total_pages else None,
        "previous_page": f"{base_url}?page={page - 1}" if page > 1 else None,
        "rows": rows_data,
    }
