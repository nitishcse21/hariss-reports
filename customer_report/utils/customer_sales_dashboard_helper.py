from datetime import datetime




def choose_granularity(from_date_str: str, to_date_str: str):
    d1 = datetime.fromisoformat(str(from_date_str)).date()
    d2 = datetime.fromisoformat(str(to_date_str)).date()
    days = (d2 - d1).days + 1

    if days <= 31:
        # daily
        granularity = "daily"
        period_label_sql = "TO_CHAR(invoice_date, 'YYYY-MM-DD')"
        order_by_sql = "invoice_date"

    elif days <= 183:
        # weekly
        granularity = "weekly"
        period_label_sql = """
        CONCAT(
            TO_CHAR(DATE_TRUNC('week', invoice_date), 'DD Mon'),
            ' - ',
            TO_CHAR(DATE_TRUNC('week', invoice_date) + INTERVAL '6 days', 'DD Mon')
        )
        """
        order_by_sql = "DATE_TRUNC('week', invoice_date)"

    else:
        # monthly
        granularity = "monthly"
        period_label_sql = "TO_CHAR(DATE_TRUNC('month', invoice_date), 'YYYY-MM')"
        order_by_sql = "DATE_TRUNC('month', invoice_date)"

    return granularity, period_label_sql, order_by_sql


def ensure_warehouse_join(joins: list[str]):
    wh_join = "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
    if wh_join not in joins:
        joins.append(wh_join)


def customer_level_filter(level: str) -> str:
    if level == "route":
        return "ac.route_id = ANY(:route_ids)"
    elif level == "warehouse":
        return "ac.warehouse = ANY(:warehouse_ids)"
    elif level == "area":
        return "ac.warehouse IN (SELECT id FROM tbl_warehouse WHERE area_id = ANY(:area_ids))"
    elif level == "region":
        return "ac.warehouse IN (SELECT id FROM tbl_warehouse WHERE region_id = ANY(:region_ids))"
    else:
        return "1=1"