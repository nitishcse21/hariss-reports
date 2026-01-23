from datetime import datetime, date, timedelta
from typing import List, Tuple, Dict
from app.sales_report.schemas.sales_schema import SalesReportRequest
import re


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


def write_aggregated_sheet(
    ws,
    map_items: Dict[Tuple[str, str, str], Dict[str, float]],
    sorted_periods: List[str],
    header_fmt,
    number_fmt,
    category_label_fmt,
    bold_red_fmt,
    grand_total_fmt
):

    # Convert weekly labels (YYYY-MM-DD_to_YYYY-MM-DD) → pretty format
    def format_week_display(label):
        if "_to_" not in label:
            return label
        start, end = label.split("_to_")
        s = datetime.strptime(start, "%Y-%m-%d").strftime("%d %b")
        e = datetime.strptime(end, "%Y-%m-%d").strftime("%d %b")
        return f"{s} - {e}"
    pretty_periods = [format_week_display(p) for p in sorted_periods]

    # Header row
    header = ["Item Code", "Item Name", "Material Category"] + pretty_periods + ["Total"]
    ws.write_row(0, 0, header, header_fmt)

    r = 1

    # Sort items by category then name
    sorted_items = sorted(map_items.items(), key=lambda kv: (kv[0][2], kv[0][1], kv[0][0]))

    # ---------------- NORMAL ITEM ROWS ----------------
    for (item_code, item_name, category), per_map in sorted_items:
        total_row = sum(float(per_map.get(p, 0.0)) for p in sorted_periods)

        ws.write(r, 0, item_code)
        ws.write(r, 1, item_name)
        ws.write(r, 2, category)

        col = 3
        for p in sorted_periods:
            ws.write_number(r, col, float(per_map.get(p, 0.0)), number_fmt)
            col += 1

        ws.write_number(r, col, float(total_row), number_fmt)
        r += 1

    # ---------------- CATEGORY TOTALS ----------------
    from collections import defaultdict
    cat_totals = defaultdict(lambda: {p: 0.0 for p in sorted_periods})

    for (item_code, item_name, category), per_map in map_items.items():
        for p in sorted_periods:
            cat_totals[category][p] += float(per_map.get(p, 0.0))

    r += 1  # blank row

    for cat, per_map in cat_totals.items():
        ws.write(r, 0, "")
        ws.write(r, 1, cat, category_label_fmt)
        ws.write(r, 2, "")

        col = 3
        total_cat = 0.0
        for p in sorted_periods:
            v = float(per_map[p])
            ws.write_number(r, col, v, bold_red_fmt)
            total_cat += v
            col += 1

        ws.write_number(r, col, total_cat, bold_red_fmt)
        r += 1

    # ---------------- GRAND TOTAL ----------------
    grand_totals = {p: 0.0 for p in sorted_periods}
    for per_map in map_items.values():
        for p in sorted_periods:
            grand_totals[p] += float(per_map.get(p, 0.0))

    ws.write(r, 0, "")
    ws.write(r, 1, "Total", grand_total_fmt)
    ws.write(r, 2, "")

    col = 3
    for p in sorted_periods:
        ws.write_number(r, col, float(grand_totals[p]), grand_total_fmt)
        col += 1

    ws.write_number(r, col, float(sum(grand_totals.values())), grand_total_fmt)

    # Autosize
    for i in range(len(header)):
        ws.set_column(i, i, 15)


def generate_week_ranges(from_date, to_date):
    """
    Generates weekly ranges starting from the exact from_date.
    Each week = 7 days, last week may be shorter.
    """
    ranges = []
    cur = from_date

    while cur <= to_date:
        end = cur + timedelta(days=6)
        if end > to_date:
            end = to_date

        ranges.append((cur, end))
        cur = end + timedelta(days=1)

    return ranges


def _safe_sheet_name(name: str) -> str:
    if not name:
        return "Unknown"
    safe = re.sub(r'[\\/*?:\[\]]', '_', name)
    safe = re.sub(r'\s+', ' ', safe).strip()
    # truncate to 31 chars
    return safe[:31]



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
def determine_default_entities(payload, name_maps):
    """
    Determine which entity level is the lowest selected, based on your hierarchy.
    Returns: (entity_type, list_of_ids, name_map_for_that_entity)
    This version is resilient: if name_maps doesn't have friendly names for the selected
    entity, it will produce fallback mapping id -> str(id) so code won't KeyError.
    """

    # helper to get ids safely (ensure list)
    def _ids(attr):
        v = getattr(payload, attr, None)
        return list(v) if v else []

    # helper to return a name_map for a key (fallback to id->str(id))
    def _ensure_map(key, ids):
        nm = name_maps.get(key)
        if not nm:
            # build a minimal fallback map so we don't KeyError later
            nm = {eid: str(eid) for eid in ids} if ids else {}
        else:
            # ensure all ids exist in nm (fallback to str)
            for eid in ids:
                if eid not in nm:
                    nm[eid] = str(eid)
        return nm

    # 1. customer
    customer_ids = _ids("customer_ids")
    if customer_ids:
        return ("customer", customer_ids, _ensure_map("customer", customer_ids))

    # 2. customer_category
    cc_ids = _ids("customer_category_ids")
    if cc_ids:
        return ("customer_category", cc_ids, _ensure_map("customer_category", cc_ids))

    # 3. customer_channel
    ch_ids = _ids("customer_channel_ids")
    if ch_ids:
        return ("customer_channel", ch_ids, _ensure_map("customer_channel", ch_ids))

    # 4. salesman
    s_ids = _ids("salesman_ids")
    if s_ids:
        return ("salesman", s_ids, _ensure_map("salesman", s_ids))

    # 5. route
    r_ids = _ids("route_ids")
    if r_ids:
        return ("route", r_ids, _ensure_map("route", r_ids))

    # 6. warehouse
    w_ids = _ids("warehouse_ids")
    if w_ids:
        return ("warehouse", w_ids, _ensure_map("warehouse", w_ids))

    # 7. area
    a_ids = _ids("area_ids")
    if a_ids:
        return ("area", a_ids, _ensure_map("area", a_ids))

    # 8. region
    reg_ids = _ids("region_ids")
    if reg_ids:
        return ("region", reg_ids, _ensure_map("region", reg_ids))

    # 9. company (DEFAULT fallback)
    comp_ids = _ids("company_ids")
    # ensure company_ids has something (your earlier code sets [1] if empty)
    if not comp_ids:
        comp_ids = [1]
    return ("company", comp_ids, _ensure_map("company", comp_ids))


# -------------------- deepest hierarchy --------------------
def get_deepest(p: SalesReportRequest):
    if p.customer_ids:
        return "customer"
    if p.customer_category_ids:
        return "customer_category"
    if p.customer_channel_ids:
        return "customer_channel"
    if p.route_ids or p.salesman_ids:
        return "route_salesman"
    if p.warehouse_ids:
        return "warehouse"
    if p.area_ids:
        return "area"
    if p.region_ids:
        return "region"
    return "company"