from datetime import datetime
from typing import Dict, Optional, List, Tuple
from fastapi import HTTPException
from app.primary_order_report.schemas.pmry_ord_schema import PrimaryOrderReportSchema



def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None
    

def validate_mandatory(filters:PrimaryOrderReportSchema):
    if not filters.from_date or not filters.to_date:
        raise HTTPException(status_code=400, detail="from_date, to_date, and search_type are required")
    try:
        _ = datetime.fromisoformat(filters.from_date)
        _ = datetime.fromisoformat(filters.to_date)
    except Exception:
        raise HTTPException(status_code=400, detail="from_date/to_date must be in YYYY-MM-DD format")
    

def choose_granularity(from_date_str: str, to_date_str: str) -> tuple[str, str, str]:  
    d1 = datetime.fromisoformat(from_date_str).date()
    d2 = datetime.fromisoformat(to_date_str).date()
    days = (d2 - d1).days + 1

    if days <= 31:
        # day wise
        granularity = "daily"
        period_label_sql = "TO_CHAR(hth.order_date, 'YYYY-MM-DD')"
        order_by_sql = "hth.order_date"
    elif days <= 183:
        # week wise
        granularity = "weekly"
        period_label_sql =  """CONCAT(
        TO_CHAR(DATE_TRUNC('week', hth.order_date), 'DD Mon'),
        ' - ',
        TO_CHAR(DATE_TRUNC('week', hth.order_date) + INTERVAL '6 days', 'DD Mon')
    )
        """
        order_by_sql = "DATE_TRUNC('week', hth.order_date)"
    else:
        # month wise
        granularity = "monthly"
        period_label_sql = "TO_CHAR(date_trunc('month', hth.order_date), 'Mon-YYYY')"
        order_by_sql = "DATE_TRUNC('month', hth.order_date)"

    return granularity, period_label_sql, order_by_sql



def build_query_parts(
    filters: PrimaryOrderReportSchema,
) -> Tuple[List[str], List[str], Dict]:


    joins: List[str] = []
    where_fragments: List[str] = []
    params: Dict = {}

    where_fragments.append(
        "hth.order_date BETWEEN :from_date AND :to_date"
    )
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date


    if filters.company_ids:
        # joins.append(
        #     "JOIN tbl_warehouse w ON w.id = hth.warehouse_id"
        # )
        where_fragments.append(
            "hth.company_id = ANY(:company_ids)"
        )
        params["company_ids"] = filters.company_ids


    need_warehouse_join = False

    if filters.region_ids:
        joins.append(
            "JOIN tbl_warehouse w ON w.id = hth.warehouse_id"
        )
        where_fragments.append(
            "w.region_id = ANY(:region_ids)"
        )
        params["region_ids"] = filters.region_ids
        need_warehouse_join = True

    if filters.area_ids:
        if not need_warehouse_join:
            joins.append(
                "JOIN tbl_warehouse w ON w.id = hth.warehouse_id"
            )
            need_warehouse_join = True

        where_fragments.append(
            "w.area_id = ANY(:area_ids)"
        )
        params["area_ids"] = filters.area_ids

    if filters.warehouse_ids:
        where_fragments.append(
            "hth.warehouse_id = ANY(:warehouse_ids)"
        )
        params["warehouse_ids"] = filters.warehouse_ids


    joins = list(dict.fromkeys(joins))

    return joins, where_fragments, params
