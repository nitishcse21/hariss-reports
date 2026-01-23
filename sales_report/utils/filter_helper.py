from fastapi import HTTPException
from typing import List, Optional
from datetime import datetime
from typing import Tuple
from sales_report.schemas.sales_schema import FilterSelection
import json




# -------------------------
# Helpers
# -------------------------
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

# -------------------------
# Build query parts (used by dashboard/table/export)
# -------------------------
def build_query_parts(filters: FilterSelection):
    """
    Returns (joins:list[str], where_fragments:list[str], params:dict)
    Also applies display_quantity logic (exclude item_total = 0 rows when required).
    """
    joins = []
    where_fragments = []
    params = {}

    # Date filter on invoice_headers.invoice_date
    where_fragments.append("ih.invoice_date BETWEEN :from_date AND :to_date")
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    # Optionally exclude free goods (item_total = 0)
    if filters.display_quantity and filters.display_quantity.lower() == "without_free_good":
        where_fragments.append("id.item_total <> 0")

    # Company filter (company independent)
    if filters.company_ids:
        where_fragments.append("ih.company_id = ANY(:company_ids)")
        params["company_ids"] = filters.company_ids

    # Region filter: will require warehouse join
    need_warehouse_join = False
    if filters.region_ids:
        joins.append("JOIN tbl_warehouse w ON w.id = ih.warehouse_id")
        where_fragments.append("w.region_id = ANY(:region_ids)")
        params["region_ids"] = filters.region_ids
        need_warehouse_join = True

    # Area filter: needs warehouse join
    if filters.area_ids:
        if not need_warehouse_join:
            joins.append("JOIN tbl_warehouse w ON w.id = ih.warehouse_id")
            need_warehouse_join = True
        where_fragments.append("w.area_id = ANY(:area_ids)")
        params["area_ids"] = filters.area_ids

    # Warehouse filter: direct
    if filters.warehouse_ids:
        where_fragments.append("ih.warehouse_id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = filters.warehouse_ids

    # Route / Salesman filters directly on ih
    if filters.route_ids:
        where_fragments.append("ih.route_id = ANY(:route_ids)")
        params["route_ids"] = filters.route_ids
    if filters.salesman_ids:
        where_fragments.append("ih.salesman_id = ANY(:salesman_ids)")
        params["salesman_ids"] = filters.salesman_ids

    # Item / Item Category filters (join items if needed)
    # NOTE: Update table names here if your item table differs (tbl_item vs items)
    if filters.item_category_ids:
        joins.append("JOIN items itm ON itm.id = id.item_id")
        where_fragments.append("itm.category_id = ANY(:item_category_ids)")
        params["item_category_ids"] = filters.item_category_ids
    if filters.item_ids:
        where_fragments.append("id.item_id = ANY(:item_ids)")
        params["item_ids"] = filters.item_ids

    # Customer filters
    if filters.customer_channel_ids or filters.customer_category_ids or filters.customer_ids:
        joins.append("JOIN agent_customers c ON c.id = ih.customer_id")
        if filters.customer_channel_ids:
            where_fragments.append("c.outlet_channel_id = ANY(:customer_channel_ids)")
            params["customer_channel_ids"] = filters.customer_channel_ids
        if filters.customer_category_ids:
            where_fragments.append("c.category_id = ANY(:customer_category_ids)")
            params["customer_category_ids"] = filters.customer_category_ids
        if filters.customer_ids:
            where_fragments.append("ih.customer_id = ANY(:customer_ids)")
            params["customer_ids"] = filters.customer_ids

    joins = list(dict.fromkeys(joins))
    return joins, where_fragments, params

# ---------- Helpers ----------
def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None

def normalize_user_field(raw):
    """
    Normalize a 'users' field which can be:
      - None -> means NULL in DB => return ('none', [])
      - SQL array -> Python list -> if [] => ('all', []) else ('list', [..])
      - scalar int -> ('list', [int])
      - string that contains JSON array like '[1,2]' or '["86"]' -> parse and treat accordingly
    Returns tuple (mode, list)
      - mode == 'none' => no access (should cause empty downstream results)
      - mode == 'all'  => all access (do not restrict)
      - mode == 'list' => whitelist (restrict to elements in list)
    """
    if raw is None:
        return "none", []
    # If string that looks like JSON array, try parse
    if isinstance(raw, str):
        raw_strip = raw.strip()
        if raw_strip.startswith("[") and raw_strip.endswith("]"):
            try:
                parsed = json.loads(raw_strip)
                # normalize numbers in parsed to ints if possible
                parsed_clean = []
                for v in parsed:
                    try:
                        parsed_clean.append(int(v))
                    except Exception:
                        # skip or ignore non-int entries
                        pass
                if len(parsed_clean) == 0:
                    return "all", []
                return "list", parsed_clean
            except Exception:
                # fallback: try to parse comma separated ints
                parts = [p.strip() for p in raw_strip.strip("[]").split(",") if p.strip() != ""]
                try:
                    vals = [int(p) for p in parts]
                    if len(vals) == 0:
                        return "all", []
                    return "list", vals
                except Exception:
                    return "none", []
        # otherwise try single int string
        try:
            v = int(raw_strip)
            return "list", [v]
        except Exception:
            return "none", []
    # If already a list
    if isinstance(raw, (list, tuple)):
        vals = []
        for v in raw:
            try:
                vals.append(int(v))
            except Exception:
                continue
        if len(vals) == 0:
            # Empty array in DB -> means "all" per your rule
            return "all", []
        return "list", vals
    # if scalar numeric
    try:
        return "list", [int(raw)]
    except Exception:
        return "none", []

def resolve_effective_ui_vs_user(ui_list: Optional[List[int]],
                                 user_mode: str, user_list: List[int]) -> Tuple[str, Optional[List[int]]]:
    """
    Determine effective restriction based on UI-provided list and user allowed spec.
    Returns (mode, list_or_None):
      - mode 'none' => user has no access -> final result must be empty
      - mode 'all'  => no restriction (either user allows all and ui didn't restrict)
      - mode 'list' => restrict to given list (could be intersection or ui list)
      - list_or_None: if None -> no SQL restriction (i.e. fetch all)
                       if [] -> explicitly empty set (no rows)
                       else tuple/list to use in IN clause
    Logic:
      - If user_mode == 'none' -> return ('none', [])
      - If user_mode == 'all':
          - if ui_list provided -> return ('list', ui_list)
          - else -> return ('all', None)
      - If user_mode == 'list':
          - if ui_list provided -> return ('list', intersection)
          - else -> return ('list', user_list)
    Note: intersection might be empty -> means no access -> return ('none', [])
    """
    if user_mode == "none":
        return "none", []
    if user_mode == "all":
        if ui_list:
            return "list", ui_list
        else:
            return "all", None
    # user_mode == 'list'
    if ui_list:
        inter = [v for v in ui_list if v in user_list]
        if not inter:
            return "none", []
        return "list", inter
    # ui not provided, use user's whitelist
    return "list", user_list
