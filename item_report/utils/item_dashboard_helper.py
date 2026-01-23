from datetime import datetime


from datetime import date

def choose_granularity(from_date: date, to_date: date) -> tuple[str, str, str]:
    # from_date and to_date are already date objects
    days = (to_date - from_date).days + 1

    if days <= 31:
        # Daily
        granularity = "daily"
        period_label_sql = "TO_CHAR(ms.invoice_date, 'YYYY-MM-DD')"
        order_by_sql = "ms.invoice_date"

    elif days <= 183:
        # Weekly (Mon–Sun range label)
        granularity = "weekly"
        period_label_sql = """
        CONCAT(
            TO_CHAR(DATE_TRUNC('week', ms.invoice_date), 'DD Mon'),
            ' - ',
            TO_CHAR(DATE_TRUNC('week', ms.invoice_date) + INTERVAL '6 days', 'DD Mon')
        )
        """
        order_by_sql = "DATE_TRUNC('week', ms.invoice_date)"

    else:
        # Monthly
        granularity = "monthly"
        period_label_sql = "TO_CHAR(DATE_TRUNC('month', ms.invoice_date), 'Mon-YYYY')"
        order_by_sql = "DATE_TRUNC('month', ms.invoice_date)"

    return granularity, period_label_sql, order_by_sql


UPC_JOIN = """
LEFT JOIN (
    SELECT item_id, MAX(upc::numeric) AS upc
    FROM item_uoms
    WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
    GROUP BY item_id
) iu ON iu.item_id = ms.item_id
"""

