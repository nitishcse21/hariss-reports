from datetime import datetime


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


def ensure_warehouse_join(joins: list[str]):
    wh_join = "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
    if wh_join not in joins:
        joins.append(wh_join)


