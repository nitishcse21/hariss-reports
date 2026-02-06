from typing import Optional, List
from datetime import date


def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None
    


def choose_granularity(from_date: date, to_date: date, date_col: str):
    days = (to_date - from_date).days + 1

    if days <= 31:
        granularity = "daily"
        period_sql = f"TO_CHAR({date_col}, 'YYYY-MM-DD')"
        order_sql = date_col

    elif days <= 183:
        granularity = "weekly"
        period_sql = f"""
        CONCAT(
            TO_CHAR(DATE_TRUNC('week', {date_col}), 'DD Mon'),
            ' - ',
            TO_CHAR(DATE_TRUNC('week', {date_col}) + INTERVAL '6 days', 'DD Mon')
        )
        """
        order_sql = f"DATE_TRUNC('week', {date_col})"

    else:
        granularity = "monthly"
        period_sql = f"TO_CHAR(DATE_TRUNC('month', {date_col}), 'Mon-YYYY')"
        order_sql = f"DATE_TRUNC('month', {date_col})"

    return granularity, period_sql, order_sql
