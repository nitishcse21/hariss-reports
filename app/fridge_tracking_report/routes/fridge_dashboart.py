from fastapi import APIRouter
from sqlalchemy import text
from app.fridge_tracking_report.schemas.fridge_schema import FridgeTrackingRequest
from app.fridge_tracking_report.utils.fridge_helper import validate_mandatory, build_query_parts
from app.database import engine

router = APIRouter()

@router.post("/fridge-kpi")
def fridge_kpi(filters: FridgeTrackingRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)

    where_sql = " AND ".join(where_fragments)

    query = f"""
        SELECT
            COUNT(*) AS total_visits,
            COUNT(CASE WHEN ft.have_fridge = 'yes' THEN 1 END) AS fridge_yes,
            COUNT(CASE WHEN ft.have_fridge = 'no' THEN 1 END) AS fridge_no,
            COUNT(CASE WHEN ft.complaint_type IS NOT NULL THEN 1 END) AS complaint_count,
            COUNT(
                CASE WHEN tac.serial_number IS NOT NULL
                     AND ft.serial_no IS NOT NULL
                     AND tac.serial_number <> ft.serial_no
                THEN 1 END
            ) AS serial_mismatch_count
        FROM tbl_fridge_tracking_report ft
        JOIN tbl_route rt ON rt.id = ft.route_id
        JOIN agent_customers ac ON ac.id = ft.customer_id
        JOIN salesman s ON s.id = ft.salesman_id
        LEFT JOIN tbl_add_chillers tac ON tac.customer_id = ac.id
        JOIN tbl_warehouse w ON w.id = ac.warehouse
        JOIN tbl_region r ON r.id = w.region_id
        JOIN tbl_areas a ON a.id = w.area_id 
        WHERE {where_sql}
    """

    with engine.connect() as conn:
        row = conn.execute(text(query), params).mappings().first()

    total = row["total_visits"] or 0
    yes = row["fridge_yes"] or 0

    coverage = round((yes / total) * 100, 2) if total else 0

    return {
        "total_visits": total,
        "fridge_yes": yes,
        "fridge_no": row["fridge_no"],
        "coverage_percent": f"{coverage}%",
        "complaint_count": row["complaint_count"],
        "serial_mismatch_count": row["serial_mismatch_count"],
    }


@router.post("/fridge-availability-chart")

def fridge_availability_chart(filters: FridgeTrackingRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    query = f"""
        SELECT
            ft.have_fridge,
            COUNT(*) AS count
        FROM tbl_fridge_tracking_report ft
        JOIN agent_customers ac ON ac.id = ft.customer_id
        JOIN tbl_warehouse w ON w.id = ac.warehouse
        WHERE {where_sql}
        GROUP BY ft.have_fridge
    """

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return rows



@router.post("/fridge-complaint-chart")
def fridge_complaint_chart(filters: FridgeTrackingRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    query = f"""
        SELECT
            ft.complaint_type,
            COUNT(*) AS count
        FROM tbl_fridge_tracking_report ft
        JOIN agent_customers ac ON ac.id = ft.customer_id
        JOIN tbl_warehouse w ON w.id = ac.warehouse
        WHERE {where_sql}
        AND ft.complaint_type IS NOT NULL
        GROUP BY ft.complaint_type
        ORDER BY count DESC
    """

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return rows


@router.post("/fridge-map-data")
def fridge_map_data(filters: FridgeTrackingRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    query = f"""
        SELECT
            ft.outlet_name,
            ft.latitude,
            ft.longitude,
            ft.have_fridge,
            ft.complaint_type
        FROM tbl_fridge_tracking_report ft
        JOIN agent_customers ac ON ac.id = ft.customer_id
        JOIN tbl_warehouse w ON w.id = ac.warehouse
        WHERE {where_sql}
        AND ft.latitude IS NOT NULL
        AND ft.longitude IS NOT NULL
    """

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return rows
