from typing import List, Optional
from fastapi import HTTPException
from datetime import datetime
from app.dashboard.schemas.dashboard_schema import DashboardRequest


def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None
    
def validate_mandatory(filters:DashboardRequest):
    if not filters.from_date or not filters.to_date:
        raise HTTPException(status_code=400, detail="from_date and to_date are required")
    try:
        _ = datetime.fromisoformat(filters.from_date)
        _ = datetime.fromisoformat(filters.to_date)
    except Exception:
        raise HTTPException(status_code=400, detail="from_date/to_date must be in YYYY-MM-DD format")

def sales_build_query_parts(filters: DashboardRequest):
    joins = []
    where_fragments = []
    params = {}

    where_fragments.append("ih.invoice_date BETWEEN :from_date AND :to_date")
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.display_quantity and filters.display_quantity.lower() == "without_free_good":
        where_fragments.append("id.item_total <> 0")

    if filters.company_ids:
        where_fragments.append("ih.company_id = ANY(:company_ids)")
        params["company_ids"] = filters.company_ids

    need_wh_join = False
    if filters.region_ids:
        joins.append("JOIN tbl_warehouse w ON w.id = ih.warehouse_id")
        where_fragments.append("w.region_id = ANY(:region_ids)")
        params["region_ids"] = filters.region_ids
        need_wh_join = True

    if filters.area_ids:
        if not need_wh_join:
            joins.append("JOIN tbl_warehouse w ON w.id = ih.warehouse_id")
            need_wh_join = True
        where_fragments.append("w.area_id = ANY(:area_ids)")
        params["area_ids"] = filters.area_ids

    if filters.warehouse_ids:
        where_fragments.append("ih.warehouse_id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = filters.warehouse_ids

    # if filters.item_category_ids:
    #     where_fragments.append("it.category_id = ANY(:item_category_ids)")
    #     params["item_category_ids"] = filters.item_category_ids

    # if filters.customer_channel_ids:
    #     where_fragments.append("c.outlet_channel_id = ANY(:customer_channel_ids)")
    #     params["customer_channel_ids"] = filters.customer_channel_ids

    # if filters.customer_category_ids:
    #     where_fragments.append("c.category_id = ANY(:customer_category_ids)")
    #     params["customer_category_ids"] = filters.customer_category_ids

    joins = list(dict.fromkeys(joins))
    return joins, where_fragments, params

def purchase_build_query_parts(filters: DashboardRequest):
    joins = []
    where_fragments = []
    params = {}

    where_fragments.append("hih.invoice_date BETWEEN :from_date AND :to_date")
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.display_quantity and filters.display_quantity.lower() == "without_free_good":
        where_fragments.append("hid.total <> 0")

    if filters.company_ids:
        where_fragments.append("hih.company_id = ANY(:company_ids)")
        params["company_ids"] = filters.company_ids

    need_wh_join = False
    if filters.region_ids:
        joins.append("JOIN tbl_warehouse w ON w.id = hih.warehouse_id")
        where_fragments.append("w.region_id = ANY(:region_ids)")
        params["region_ids"] = filters.region_ids
        need_wh_join = True

    if filters.area_ids:
        if not need_wh_join:
            joins.append("JOIN tbl_warehouse w ON w.id = hih.warehouse_id")
            need_wh_join = True
        where_fragments.append("w.area_id = ANY(:area_ids)")
        params["area_ids"] = filters.area_ids

    if filters.warehouse_ids:
        where_fragments.append("hih.warehouse_id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = filters.warehouse_ids

    # if filters.item_category_ids:
    #     where_fragments.append("it.category_id = ANY(:item_category_ids)")
    #     params["item_category_ids"] = filters.item_category_ids

    # if filters.customer_channel_ids:
    #     where_fragments.append("c.outlet_channel_id = ANY(:customer_channel_ids)")
    #     params["customer_channel_ids"] = filters.customer_channel_ids

    # if filters.customer_category_ids:
    #     where_fragments.append("c.category_id = ANY(:customer_category_ids)")
    #     params["customer_category_ids"] = filters.customer_category_ids

    joins = list(dict.fromkeys(joins))
    return joins, where_fragments, params

def return_build_query_parts(filters: DashboardRequest):
    joins = []
    where_fragments = []
    params = {}

    where_fragments.append("hrh.created_at BETWEEN :from_date AND :to_date")
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.display_quantity and filters.display_quantity.lower() == "without_free_good":
        where_fragments.append("hrd.total <> 0")

    if filters.company_ids:
        where_fragments.append("hrh.company_id = ANY(:company_ids)")
        params["company_ids"] = filters.company_ids

    need_wh_join = False
    if filters.region_ids:
        joins.append("JOIN tbl_warehouse w ON w.id = hrh.warehouse_id")
        where_fragments.append("w.region_id = ANY(:region_ids)")
        params["region_ids"] = filters.region_ids
        need_wh_join = True

    if filters.area_ids:
        if not need_wh_join:
            joins.append("JOIN tbl_warehouse w ON w.id = hrh.warehouse_id")
            need_wh_join = True
        where_fragments.append("w.area_id = ANY(:area_ids)")
        params["area_ids"] = filters.area_ids

    if filters.warehouse_ids:
        where_fragments.append("hrh.warehouse_id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = filters.warehouse_ids

    # if filters.item_category_ids:
    #     where_fragments.append("it.category_id = ANY(:item_category_ids)")
    #     params["item_category_ids"] = filters.item_category_ids

    # if filters.customer_channel_ids:
    #     where_fragments.append("c.outlet_channel_id = ANY(:customer_channel_ids)")
    #     params["customer_channel_ids"] = filters.customer_channel_ids

    # if filters.customer_category_ids:
    #     where_fragments.append("c.category_id = ANY(:customer_category_ids)")
    #     params["customer_category_ids"] = filters.customer_category_ids

    joins = list(dict.fromkeys(joins))
    return joins, where_fragments, params

def choose_granularity(from_date_str: str, to_date_str: str) -> tuple[str, str, str]:  
    d1 = datetime.fromisoformat(from_date_str).date()
    d2 = datetime.fromisoformat(to_date_str).date()
    days = (d2 - d1).days + 1

    if days <= 31:
        granularity = "daily"
        period_label_sql = "TO_CHAR(ih.invoice_date, 'YYYY-MM-DD')"
        order_by_sql = "ih.invoice_date"

    elif days <= 183:
        granularity = "weekly"
        period_label_sql =  """CONCAT(
        TO_CHAR(DATE_TRUNC('week', ih.invoice_date), 'DD Mon'),
        ' - ',
        TO_CHAR(DATE_TRUNC('week', ih.invoice_date) + INTERVAL '6 days', 'DD Mon')
    )
        """
        order_by_sql = "DATE_TRUNC('week', ih.invoice_date)"
    else:
        # month wise
        granularity = "monthly"
        period_label_sql = "TO_CHAR(date_trunc('month', ih.invoice_date), 'Mon-YYYY')"
        order_by_sql = "DATE_TRUNC('month', ih.invoice_date)"

    return granularity, period_label_sql, order_by_sql

def choose_purchase_granularity(
    from_date_str: str,
    to_date_str: str
) -> tuple[str, str, str]:

    d1 = datetime.fromisoformat(from_date_str).date()
    d2 = datetime.fromisoformat(to_date_str).date()
    days = (d2 - d1).days + 1

    if days <= 31:
        granularity = "daily"
        period_label_sql = "TO_CHAR(hih.invoice_date, 'YYYY-MM-DD')"
        order_by_sql = "hih.invoice_date"

    elif days <= 183:
        granularity = "weekly"
        period_label_sql = """
            CONCAT(
                TO_CHAR(DATE_TRUNC('week', hih.invoice_date), 'DD Mon'),
                ' - ',
                TO_CHAR(DATE_TRUNC('week', hih.invoice_date) + INTERVAL '6 days', 'DD Mon')
            )
        """
        order_by_sql = "DATE_TRUNC('week', hih.invoice_date)"

    else:
        granularity = "monthly"
        period_label_sql = "TO_CHAR(DATE_TRUNC('month', hih.invoice_date), 'Mon-YYYY')"
        order_by_sql = "DATE_TRUNC('month', hih.invoice_date)"

    return granularity, period_label_sql.strip(), order_by_sql

def choose_return_granularity(
    from_date_str: str,
    to_date_str: str
) -> tuple[str, str, str]:
   
    d1 = datetime.fromisoformat(from_date_str).date()
    d2 = datetime.fromisoformat(to_date_str).date()
    days = (d2 - d1).days + 1

    if days <= 31:
        granularity = "daily"
        period_label_sql = "TO_CHAR(hrh.created_at, 'YYYY-MM-DD')"
        order_by_sql = "hrh.created_at"

    elif days <= 183:
        granularity = "weekly"
        period_label_sql = """
            CONCAT(
                TO_CHAR(DATE_TRUNC('week', hrh.created_at), 'DD Mon'),
                ' - ',
                TO_CHAR(DATE_TRUNC('week', hrh.created_at) + INTERVAL '6 days', 'DD Mon')
            )
        """
        order_by_sql = "DATE_TRUNC('week', hrh.created_at)"

    else:
        granularity = "monthly"
        period_label_sql = "TO_CHAR(DATE_TRUNC('month', hrh.created_at), 'Mon-YYYY')"
        order_by_sql = "DATE_TRUNC('month', hrh.created_at)"

    return granularity, period_label_sql.strip(), order_by_sql

def quantity_expr_sql():
    return """
    ROUND(
        CAST(
            SUM(
                CASE
                    WHEN id.uom IN (1,3)
                         AND NULLIF(iu.upc::numeric, 0) IS NOT NULL
                    THEN id.quantity / iu.upc::numeric
                    ELSE id.quantity
                END
            ) AS numeric
        ),
        3
    )
    """

def purchase_quantity_expr_sql():
    return """
    ROUND(
        CAST(
            SUM(
                CASE
                    WHEN hid.uom_id IN (1,3)
                        AND NULLIF(iu.upc::numeric, 0) IS NOT NULL
                    THEN hid.quantity / iu.upc::numeric
                    ELSE hid.quantity
                END
            ) AS numeric
        ),
        3
    )
    """

def return_quantity_expr_sql():
    return """
    ROUND(
        CAST(
            SUM(
               CASE
                    WHEN hrd.uom IN (1,3)
                        AND NULLIF(iu.upc::numeric, 0) IS NOT NULL
                    THEN hrd.qty / iu.upc::numeric
                    ELSE hrd.qty
                END
            ) AS numeric
        ),
        3
    )    
    """