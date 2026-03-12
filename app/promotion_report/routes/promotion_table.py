from fastapi import APIRouter, Query, Request, Depends
from app.promotion_report.schemas.promotion_schema import PromotionRequest
from app.promotion_report.utils.promotion_common_helper import (
    validate_mandatory,
    build_query_parts,
)
from app.database import engine
from sqlalchemy import text
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.report_key_validator import get_user_from_report_key

router = APIRouter()
security = HTTPBearer()


@router.post("/promotion-table")
def promotion_table(
    filters: PromotionRequest,
    request: Request,
    page: int = Query(1, ge=1),
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    report_key = credentials.credentials.strip()
    page_size = 50
    validate_mandatory(filters)
    joins, where_fragments, params, status_case = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments) if where_fragments else "1=1"
    join_sql = "\n".join(joins)

    base_sql = f"""
        FROM invoice_details idl
        JOIN invoice_headers ih ON ih.id = idl.header_id
        JOIN promotion_headers ph ON ph.id = idl.promotion_id
        JOIN salesman s ON s.id = ih.salesman_id
        JOIN tbl_company c ON c.id = ih.company_id
        JOIN tbl_route rt ON rt.id = ih.route_id
        JOIN agent_customers ac ON ac.id = ih.customer_id
        JOIN outlet_channel oc ON oc.id = ac.outlet_channel_id
        JOIN items i ON i.id = idl.item_id
        JOIN uom u ON u.id = idl.uom

        LEFT JOIN users uasm_approve ON uasm_approve.id = idl.approver_id
        LEFT JOIN users uasm_reject ON uasm_reject.id = idl.rejected_by
        LEFT JOIN roles rasm ON rasm.id = uasm_approve.role

        LEFT JOIN users ursm_approve ON ursm_approve.id = idl.rm_approver_id
        LEFT JOIN users ursm_reject ON ursm_reject.id = idl.rm_reject_id
        LEFT JOIN roles rrsm ON rrsm.id = ursm_approve.role
        {join_sql}
        WHERE {where_sql}
    """

    count_sql = f"SELECT COUNT(*) {base_sql}"

    offset = (page - 1) * page_size

    params["limit"] = page_size
    params["offset"] = offset

    query = f"""
        SELECT     
        idl.id,
        ph.promotion_name,
        ih.invoice_date,
        ih.invoice_time AS sync_time,
        ih.invoice_number,
        s.name AS salesman_name,
        w.warehouse_code || ' - ' || w.warehouse_name AS warehouse_name,
        c.company_name,
        rt.route_code || ' - ' || rt.route_name AS route_name,
        oc.outlet_channel AS outlet_name,
        ac.contact_no AS customer_contact,
        rasm.name AS area_manager,
        rrsm.name AS regional_manager,
        ac.latitude AS customer_latitude,
        ac.longitude AS customer_longitude,
        w.latitude AS warehouse_latitude,
        w.longitude AS warehouse_longitude,
        ac.district AS customer_district,
        i.code || ' - ' || i.name AS item_name,
        idl.quantity,
        u.name AS uom,
        ih.total_amount AS invoice_total,
        ih.purchaser_name,
        ih.purchaser_contact,
        {status_case} AS status
        {base_sql}
        ORDER BY ih.invoice_date DESC, ih.invoice_time DESC
        LIMIT :limit OFFSET :offset
    """

    with engine.connect() as conn:

        total_rows = conn.execute(text(count_sql), params).scalar()

        rows = conn.execute(text(query), params).fetchall()

        user_id = get_user_from_report_key(report_key, conn)
        role_id = conn.execute(
            text(
                """
            SELECT role
            FROM users
            WHERE id = :user_id
        """
            ),
            {"user_id": user_id},
        ).scalar()

    rows_data = [dict(r._mapping) for r in rows]

    total_pages = (total_rows + page_size - 1) // page_size

    base_url = str(request.url).split("?")[0]

    has_permission = role_id in [91, 92]  # ASM and RSM
    return {
        "pagination": {
            "total_rows": total_rows,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": page_size,
            "next_page": f"{base_url}?page={page + 1}" if page < total_pages else None,
            "prev_page": f"{base_url}?page={page - 1}" if page > 1 else None,
        },
        "has_permission": has_permission,
        "data": rows_data,
    }
