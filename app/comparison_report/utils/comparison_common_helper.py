from typing import Optional, List, Tuple, Dict
from app.comparison_report.schemas.comparison_schema import ComparisonRequest
from fastapi import HTTPException
from datetime import datetime, date, timedelta
import calendar


def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None


def validate_mandatory(filters: ComparisonRequest):
    if not filters.report_by or not filters.selected_date or not filters.search_type:
        raise HTTPException(
            status_code=400,
            detail="report_by, selected_date, and search_type are required"
        )

    if filters.report_by not in ["day", "month", "year"]:
        raise HTTPException(
            status_code=400,
            detail="report_by must be one of: day, month, year"
        )

    try:
        if isinstance(filters.selected_date, str):
            datetime.fromisoformat(filters.selected_date)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="selected_date must be in YYYY-MM-DD format"
        )



def build_query_parts(
    filters: ComparisonRequest,
    prev_from: date,
    current_to: date
) -> Tuple[List[str], List[str], Dict]:

    joins: List[str] = []
    where_fragments: List[str] = []
    params: Dict = {}

    # Global window
    where_fragments.append(
        "ih.invoice_date BETWEEN :prev_from AND :current_to"
    )

    params["prev_from"] = prev_from
    params["current_to"] = current_to

    if (
        filters.display_quantity
        and filters.display_quantity.lower() == "without_free_good"
    ):
        where_fragments.append("id.item_total > 0")

    if filters.warehouse_ids:
        where_fragments.append("ih.warehouse_id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = filters.warehouse_ids

    if filters.salesman_ids:
        where_fragments.append("ih.salesman_id = ANY(:salesman_ids)")
        params["salesman_ids"] = filters.salesman_ids

    return joins, where_fragments, params


def quantity_expr_sql(date_condition: str) -> str:
    return f"""
    ROUND(
        CAST(
            SUM(
                CASE
                    WHEN {date_condition}
                    THEN
                        CASE
                            WHEN id.uom IN (1,3)
                                 AND NULLIF(iu.upc::numeric, 0) IS NOT NULL
                            THEN id.quantity / iu.upc::numeric
                            ELSE id.quantity
                        END
                    ELSE 0
                END
            ) AS numeric
        ),
        3
    )
    """


def get_periods(report_by: str, selected_date: date):
    if report_by == "day":
        current_from = selected_date
        current_to = selected_date

        previous_from = selected_date - timedelta(days=1)
        previous_to = selected_date - timedelta(days=1)

    elif report_by == "month":
        current_from = selected_date.replace(day=1)
        last_day = calendar.monthrange(selected_date.year, selected_date.month)[1]
        current_to = selected_date.replace(day=last_day)

        prev_month = current_from - timedelta(days=1)
        previous_from = prev_month.replace(day=1)
        last_day_prev = calendar.monthrange(prev_month.year, prev_month.month)[1]
        previous_to = prev_month.replace(day=last_day_prev)

    elif report_by == "year":
        current_from = date(selected_date.year, 1, 1)
        current_to = date(selected_date.year, 12, 31)

        previous_from = date(selected_date.year - 1, 1, 1)
        previous_to = date(selected_date.year - 1, 12, 31)

    else:
        raise ValueError("Invalid report_by")

    return current_from, current_to, previous_from, previous_to