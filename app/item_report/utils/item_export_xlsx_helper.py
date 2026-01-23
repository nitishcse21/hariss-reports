from datetime import datetime, date
from app.item_report.schemas.item_schema import FilterSelection



def choose_granularity(min_date: date, max_date: date) -> str:
    span_days = (max_date - min_date).days
    if span_days <= 62:
        return "daily"
    elif span_days <= 183:
        return "weekly"
    else:
        return "monthly"


def sort_periods(periods, gran):
    if gran == "weekly":
        return sorted(periods, key=lambda x: datetime.strptime(x.split("_to_")[0], "%Y-%m-%d"))
    if gran == "monthly":
        return sorted(periods, key=lambda x: datetime.strptime(x, "%Y-%m"))
    if gran == "yearly":
        return sorted(periods, key=lambda x: datetime.strptime(x, "%Y"))
    return sorted(periods, key=lambda x: datetime.strptime(x, "%Y-%m-%d"))


def iso_week_to_range(iso_label: str) -> str:
        """
        Convert '2024-01' -> '2024-01-01_to_2024-01-07'
        """
        try:
            year, week = map(int, iso_label.split("-"))
            start = datetime.fromisocalendar(year, week, 1).date()
            end = datetime.fromisocalendar(year, week, 7).date()
            return f"{start}_to_{end}"
        except:
            return iso_label



def clip_period_to_range(period: str, gran: str, from_date: date, to_date: date) -> str:
    """
    Clips a period label so it never exceeds from_date / to_date
    """

    # weekly: YYYY-MM-DD_to_YYYY-MM-DD
    if gran == "weekly" and "_to_" in period:
        s, e = period.split("_to_")
        start = max(datetime.strptime(s, "%Y-%m-%d").date(), from_date)
        end = min(datetime.strptime(e, "%Y-%m-%d").date(), to_date)
        return f"{start}_to_{end}"

    # daily: YYYY-MM-DD
    if gran == "daily":
        d = datetime.strptime(period, "%Y-%m-%d").date()
        if from_date <= d <= to_date:
            return period
        return None

    # monthly: YYYY-MM
    if gran == "monthly":
        return period  # already safe by SQL grouping

    # yearly: YYYY
    if gran == "yearly":
        return period

    return period



def format_period_label(period: str, gran: str) -> str:
    """
    Converts period labels into user-friendly display format
    """

    if gran == "weekly" and "_to_" in period:
        s, e = period.split("_to_")
        start = datetime.strptime(s, "%Y-%m-%d")
        end = datetime.strptime(e, "%Y-%m-%d")
        return f"{start.strftime('%d %b')} - {end.strftime('%d %b')}"

    if gran == "daily":
        d = datetime.strptime(period, "%Y-%m-%d")
        return d.strftime("%d %b")

    if gran == "monthly":
        return datetime.strptime(period, "%Y-%m").strftime("%b %Y")

    if gran == "yearly":
        return period

    return period




# -------------------- deepest hierarchy --------------------
def get_deepest(p: FilterSelection):
    if p.route_ids:
        return "route"
    if p.warehouse_ids:
        return "warehouse"
    if p.area_ids:
        return "area"
    if p.region_ids:
        return "region"
    return "company"
