from fastapi import APIRouter, Query, Request
from sqlalchemy import text
from app.database import engine
from app.visit_report.schemas.visit_schema import VisitSchema
from app.visit_report.utils.visit_common_helper import validate_mandatory, build_query_parts

router = APIRouter()

@router.post("/visit-table")
def visit_table(filters: VisitSchema, request:Request, page: int = Query(1, ge=1)):
    validate_mandatory(filters)
    joins, where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)
    join_sql_base = "\n".join(joins)

    ROWS_PER_PAGE = 50

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
        
        count_sql = f"SELECT COUNT(*) {base_sql}"
        total_rows = conn.execute(text(count_sql), params).scalar()

        offset = (page - 1) * ROWS_PER_PAGE
        params['limit'] = ROWS_PER_PAGE
        params['offset'] = offset

        
        sql = f"""
            SELECT
            ac.osa_code || '-' || ac.name AS customer_name,
            ac.contact_no AS customer_contact,
            ac.district AS customer_district,
            s.osa_code || '-' || s.name AS salesman_name,
            st.salesman_type_name AS salesman_role,
            w.warehouse_code || '-' || w.warehouse_name AS depot_name,
            TO_CHAR(vp.visit_start_time, 'YYYY-MM-DD') AS visit_start_date,
            TO_CHAR(vp.visit_end_time, 'YYYY-MM-DD') AS visit_end_date,
            TO_CHAR(vp.visit_start_time, 'HH24:MI:SS') AS visit_start_time,
            TO_CHAR(vp.visit_end_time, 'HH24:MI:SS') AS visit_end_time,
            vp.shop_status,
            vp.latitude,
            vp.longitude
            {base_sql}
            ORDER BY vp.created_at DESC
            LIMIT :limit OFFSET :offset
        """

        result = conn.execute(text(sql), params)
        rows = [dict(r._mapping) for r in result]
        total_pages = (total_rows + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE
        base_url = str(request.url).split("?")[0]

        out = {
            "total_rows": total_rows,
            "total_pages": total_pages,
            "current_page": page,
            "next_page_url": f"{base_url}?page={page + 1}" if page < total_pages else None,
            "prev_page_url": f"{base_url}?page={page - 1}" if page > 1 else None,
            "data": rows,
        }
    return out
