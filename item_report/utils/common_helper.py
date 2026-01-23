from typing import Optional, List
from datetime import datetime
from item_report.schemas.item_schema import FilterSelection
from fastapi import HTTPException
from typing import Optional, List, Dict,Tuple

def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None


def validate_mandatory(filters: FilterSelection):
    if not filters.from_date or not filters.to_date or not filters.search_type:
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
        period_label_sql = "TO_CHAR(ih.invoice_date, 'YYYY-MM-DD')"
        order_by_sql = "ih.invoice_date"
    elif days <= 183:
        # week wise
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


def build_query_parts(
    filters: FilterSelection,
) -> Tuple[List[str], List[str], Dict]:
 

    joins: List[str] = []
    where_fragments: List[str] = []
    params: Dict = {}


    where_fragments.append(
        "ih.invoice_date BETWEEN :from_date AND :to_date"
    )
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.display_quantity and filters.display_quantity.lower() == "without_free_good":
        where_fragments.append("id.item_total > 0")


    if filters.company_ids:
        # joins.append(
        #     "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
        # )
        where_fragments.append(
            "ih.company_id = ANY(:company_ids)"
        )
        params["company_ids"] = filters.company_ids


    need_warehouse_join = False

    if filters.region_ids:
        joins.append(
            "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
        )
        where_fragments.append(
            "w.region_id = ANY(:region_ids)"
        )
        params["region_ids"] = filters.region_ids
        need_warehouse_join = True

    if filters.area_ids:
        if not need_warehouse_join:
            joins.append(
                "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
            )
            need_warehouse_join = True

        where_fragments.append(
            "w.area_id = ANY(:area_ids)"
        )
        params["area_ids"] = filters.area_ids

    if filters.warehouse_ids:
        where_fragments.append(
            "ih.warehouse_id = ANY(:warehouse_ids)"
        )
        params["warehouse_ids"] = filters.warehouse_ids


    if filters.route_ids:
        where_fragments.append(
            "ih.route_id = ANY(:route_ids)"
        )
        params["route_ids"] = filters.route_ids
    
    if filters.item_category_ids:
        where_fragments.apppend("it.category_id = ANY(:item_category_ids)")
        params["item_category_ids"] = filters.item_category_ids
    

    if filters.item_ids:
        where_fragments.append("id.item_id = ANY(:item_ids)")
        params["item_ids"] = filters.item_ids

    joins = list(dict.fromkeys(joins))

    return joins, where_fragments, params



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
    SUM(
        CASE
            WHEN hid.uom_id IN (1, 3) THEN
                CASE
                    WHEN COALESCE(NULLIF(iu.upc, ''), '0')::numeric <> 0
                        THEN ROUND(hid.quantity::numeric / COALESCE(NULLIF(iu.upc, ''), '1')::numeric, 3)
                    ELSE ROUND(hid.quantity::numeric, 3)
                END
            WHEN hid.uom_id IN (2, 4) THEN ROUND(hid.quantity::numeric, 3)
            ELSE ROUND(hid.quantity::numeric, 3)
        END
    )
    """






