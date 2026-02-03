from typing import List, Optional
from fastapi import HTTPException
from app.visit_report.schemas.visit_schema import VisitSchema
from datetime import datetime
from typing import Tuple, Dict


def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None


def validate_mandatory(filters: VisitSchema):
    if not filters.from_date or not filters.to_date:
        raise HTTPException(
            status_code=400, detail="from_date and to_date are required"
        )
    try:
        _ = datetime.fromisoformat(filters.from_date)
        _ = datetime.fromisoformat(filters.to_date)
    except Exception:
        raise HTTPException(
            status_code=400, detail="from_date/to_date must be in YYYY-MM-DD format"
        )


def choose_granularity(from_date_str: str, to_date_str: str) -> tuple[str, str, str]:
    d1 = datetime.fromisoformat(from_date_str).date()
    d2 = datetime.fromisoformat(to_date_str).date()
    days = (d2 - d1).days + 1

    if days <= 31:
        # day wise
        granularity = "daily"
        period_label_sql = "TO_CHAR(vp.visit_start_time, 'YYYY-MM-DD')"
        order_by_sql = "vp.visit_start_time"
    elif days <= 183:
        # week wise
        granularity = "weekly"
        period_label_sql = """CONCAT(
        TO_CHAR(DATE_TRUNC('week', vp.visit_start_time), 'DD Mon'),
        ' - ',
        TO_CHAR(DATE_TRUNC('week', vp.visit_start_time) + INTERVAL '6 days', 'DD Mon')
    )
        """
        order_by_sql = "DATE_TRUNC('week', vp.visit_start_time)"
    else:
        # month wise
        granularity = "monthly"
        period_label_sql = "TO_CHAR(date_trunc('month', vp.visit_start_time), 'Mon-YYYY')"
        order_by_sql = "DATE_TRUNC('month', vp.visit_start_time)"

    return granularity, period_label_sql, order_by_sql


def build_query_parts(
    filters: VisitSchema,
) -> Tuple[List[str], List[str], Dict]:

    joins: List[str] = []
    where_fragments: List[str] = []
    params: Dict = {}

    where_fragments.append("vp.visit_start_time BETWEEN :from_date AND :to_date")
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.warehouse_ids:
        where_fragments.append("vp.warehouse_id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = filters.warehouse_ids

    if filters.route_ids:
        where_fragments.append("vp.route_id = ANY(:route_ids)")
        params["route_ids"] = filters.route_ids

    if filters.salesman_ids:
        where_fragments.append("vp.salesman_id = ANY(:salesman_ids)")
        params["salesman_ids"] = filters.salesman_ids

    joins = list(dict.fromkeys(joins))

    return joins, where_fragments, params


def build_customer_filter_parts(filters:VisitSchema) -> Tuple[str, Dict]:
    where_fragments = []
    params = {}
    
    where_fragments.append("sh.requested_date BETWEEN :from_date AND :to_date")
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.warehouse_ids:
        where_fragments.append("sh.warehouse_id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = filters.warehouse_ids

    
    if filters.route_ids:
        where_fragments.append("sh.route_id = ANY(:route_ids)")
        params["route_ids"] = filters.route_ids

    if filters.salesman_ids:
        where_fragments.append("sh.salesman_id = ANY(:salesman_ids)")
        params["salesman_ids"] = filters.salesman_ids

    where_sql = " AND ".join(where_fragments)
    return where_sql, params
