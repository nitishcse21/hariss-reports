from fastapi import APIRouter
import asyncpg
from sales_report.schemas.sales_schema import SalesReportRequest
from fastapi import  HTTPException, Response
from datetime import datetime
import io
import xlsxwriter
from database import DB_URL
from sales_report.utils.export_xlsx_helper import (choose_granularity,write_aggregated_sheet, _safe_sheet_name, sort_periods, iso_week_to_range, get_deepest, determine_default_entities)
import re
from datetime import datetime


app = APIRouter()







@app.post("/sales-report-export")
async def export_dynamic_report(payload: SalesReportRequest):
    try:
        from_date = datetime.strptime(payload.from_date, "%Y-%m-%d").date()
        to_date = datetime.strptime(payload.to_date, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Dates must be YYYY-MM-DD")

   
    search_type = (payload.search_type or "quantity").lower()
    if search_type not in ("quantity", "amount"):
        raise HTTPException(status_code=400, detail="search_type must be 'quantity' or 'amount'")

   

    if search_type == "quantity":
        value_col = """
            ROUND(
                CAST(
                    SUM(
                        CASE
                            WHEN ms.uom IN (1, 3)
                                AND iu.upc IS NOT NULL
                                AND iu.upc > 0
                            THEN ms.total_quantity / iu.upc
                            ELSE ms.total_quantity
                        END
                    ) AS NUMERIC
                ),
                3
            )
        """
    else:
        value_col = "SUM(ms.total_amount)"




    if not payload.company_ids:
        payload.company_ids = [1]

    allowed = ["daily", "weekly", "monthly", "yearly"]

    # Always compute this safely first
    is_default_view = (
        payload.dataview is None or 
        payload.dataview.strip().lower() == "default"
    )

    # Now select dataview
    if payload.dataview and payload.dataview.lower() in allowed:
        dataview = payload.dataview.lower()
    else:
        dataview = choose_granularity(from_date, to_date)

    period_column = {
        "daily": "daily_period",
        "weekly": "weekly_period",
        "monthly": "monthly_period",
        "yearly": "yearly_period"
    }[dataview]


    deep = get_deepest(payload)

    # -------------------- grouping columns --------------------
    if deep == "warehouse":
        group_cols = ["ms.item_id", "ms.item_code", "ms.item_name", "ms.item_category_name", "ms.warehouse_id", period_column]
        entity_col = "warehouse_id"

    elif deep == "route_salesman":
        group_cols = ["ms.item_id", "ms.item_code", "ms.item_name", "ms.item_category_name", "ms.route_id", "ms.salesman_id", period_column]
        entity_col = None

    elif deep == "region":
        group_cols = ["ms.item_id", "ms.item_code", "ms.item_name", "ms.item_category_name", "ms.region_id", period_column]
        entity_col = "region_id"

    elif deep == "area":
        group_cols = ["ms.item_id", "ms.item_code", "ms.item_name", "ms.item_category_name", "ms.area_id", period_column]
        entity_col = "area_id"

    elif deep == "customer_channel":
        group_cols = ["ms.item_id", "ms.item_code", "ms.item_name", "ms.item_category_name", "ms.channel_id", period_column]
        entity_col = "channel_id"

    elif deep == "customer_category":
        group_cols = ["ms.item_id", "ms.item_code", "ms.item_name", "ms.item_category_name", "ms.customer_category_id", period_column]
        entity_col = "customer_category_id"

    elif deep == "customer":
        group_cols = ["ms.item_id", "ms.item_code", "ms.item_name", "ms.item_category_name", "ms.customer_id", period_column]
        entity_col = "customer_id"

    else: 
        group_cols = ["ms.company_id", "ms.item_id", "ms.item_code", "ms.item_name", "ms.item_category_name", period_column]
        entity_col = "company_id"

    select_cols = ", ".join(group_cols)
    group_by = ", ".join(group_cols)

    # -------------------- WHERE clause --------------------
    where_parts = ["ms.invoice_date BETWEEN $1 AND $2"]
    params = [from_date, to_date]
    idx = 3

    def _add(f, vals):
        nonlocal idx
        if vals:
            where_parts.append(f"{f} = ANY(${idx})")
            params.append(vals)
            idx += 1


    _add("ms.company_id", payload.company_ids)
    _add("ms.region_id", payload.region_ids)
    _add("ms.area_id", payload.area_ids)
    _add("ms.warehouse_id", payload.warehouse_ids)
    _add("ms.route_id", payload.route_ids)
    _add("ms.salesman_id", payload.salesman_ids)
    _add("ms.item_id", payload.item_ids)
    _add("ms.channel_id", payload.customer_channel_ids)
    _add("ms.customer_category_id", payload.customer_category_ids)
    _add("ms.customer_id", payload.customer_ids)





    if payload.item_category_ids:
        where_parts.append(f"ms.item_category_id = ANY(${idx})")
        params.append(payload.item_category_ids)
        idx += 1

    # -------------------- FREE-GOOD FILTER --------------------
   
    # if search_type == "quantity" AND display_quantity == "without_free_good"
    # then remove rows where total_amount = 0
    if search_type == "quantity" and getattr(payload, "display_quantity", None) == "without_free_good":
        where_parts.append("total_amount > 0")

    where_clause = " AND ".join(where_parts)






    sql = f"""
        SELECT
            {select_cols},
            {value_col} AS total_value
        FROM mv_sales_report_fast ms
        LEFT JOIN (
            SELECT
                item_id,
                MAX(upc::numeric) AS upc
            FROM item_uoms
            WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
            GROUP BY item_id
        ) iu
            ON iu.item_id = ms.item_id
        WHERE {where_clause}
        GROUP BY {group_by}
        ORDER BY ms.item_code, {period_column}

    """

    conn = await asyncpg.connect(DB_URL)
    rows = await conn.fetch(sql, *params)
    await conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    # -------------------- BUILD DICTIONARIES --------------------
    summary_map = {}
    company_map = {}
    entity_map = {}
    periods_set = set()

  
    for r in rows:
        item_id = r["item_id"]
        item_code = r["item_code"]
        item_name = r["item_name"]
        item_cat = r["item_category_name"]

        # NEW display name: "item_code + name"
        display_item = f"{item_code} - {item_name}"

        # key becomes (item_id, display_item, category)
        key = (item_id, display_item, item_cat)

        raw_period = r[period_column]

        if dataview == "weekly":
            wk = iso_week_to_range(raw_period)

            # try a few common separators
            if " to " in wk:
                parts = wk.split(" to ")
            elif "_to_" in wk:
                parts = wk.split("_to_")
            elif " - " in wk:
                parts = wk.split(" - ")
            elif "_" in wk and wk.count("_") >= 1:
                parts = wk.split("_")
            elif "/" in wk:
                parts = wk.split("/")
            else:
                parts = [wk]

            # helper to parse various date formats robustly
            def _try_parse_date(s):
                s = s.strip()
                fmts = ("%Y-%m-%d", "%d %b %Y", "%d %b", "%d-%b-%Y", "%d-%b")
                for fmt in fmts:
                    try:
                        dt = datetime.strptime(s, fmt).date()
                        # if parsed without year (like "%d %b"), assume from_date.year
                        if fmt in ("%d %b", "%d-%b"):
                            return dt.replace(year=from_date.year)
                        return dt
                    except Exception:
                        continue
                # final fallback: try extracting yyyy-mm-dd pattern
                m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
                if m:
                    try:
                        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
                    except Exception:
                        pass
                return None

            if len(parts) >= 2:
                start_s, end_s = parts[0].strip(), parts[1].strip()
                start_dt = _try_parse_date(start_s) or from_date
                end_dt = _try_parse_date(end_s) or to_date

                # Truncate end to the requested to_date (so final week won't overshoot)
                if end_dt > to_date:
                    end_dt = to_date

                period_final = f"{start_dt.strftime('%Y-%m-%d')}_to_{end_dt.strftime('%Y-%m-%d')}"
            else:
                # Could not split - fallback to raw string (safe)
                period_final = str(wk)

        else:
            period_final = str(raw_period)

        val = float(r["total_value"] or 0.0)

        periods_set.add(period_final)

        summary_map.setdefault(key, {}).setdefault(period_final, 0.0)
        summary_map[key][period_final] += val


        company_map.setdefault(key, {}).setdefault(period_final, 0.0)
        company_map[key][period_final] += val

        
        if deep == "route_salesman":
            if payload.route_ids:
                rid = r.get("route_id")
                if rid:
                    entity_map.setdefault(("route", rid), {}).setdefault(key, {}).setdefault(period_final, 0.0)
                    entity_map[("route", rid)][key][period_final] += val
            if payload.salesman_ids:
                sid = r.get("salesman_id")
                if sid:
                    entity_map.setdefault(("salesman", sid), {}).setdefault(key, {}).setdefault(period_final, 0.0)
                    entity_map[("salesman", sid)][key][period_final] += val
        elif entity_col:
            ent = r.get(entity_col)
            if ent is not None:
                entity_map.setdefault(ent, {}).setdefault(key, {}).setdefault(period_final, 0.0)
                entity_map[ent][key][period_final] += val

    sorted_periods = sort_periods(list(periods_set), dataview)

    name_maps = {}
    conn2 = await asyncpg.connect(DB_URL)
    try:
        if deep == "company":
            ids = payload.company_ids
            if ids:
                rws = await conn2.fetch("SELECT id, company_name FROM tbl_company WHERE id = ANY($1)", ids)
                name_maps = {r["id"]: r["company_name"] for r in rws}

        if deep == "region":
            ids = list(entity_map.keys())
            if ids:
                rws = await conn2.fetch("SELECT id, region_name FROM tbl_region WHERE id = ANY($1)", ids)
                name_maps = {r["id"]: r["region_name"] for r in rws}

        if deep == "area":
            ids = list(entity_map.keys())
            if ids:
                rws = await conn2.fetch("SELECT id, area_name FROM tbl_areas WHERE id = ANY($1)", ids)
                name_maps = {r["id"]: r["area_name"] for r in rws}

        if deep == "warehouse":
            ids = list(entity_map.keys())
            if ids:
                rws = await conn2.fetch("SELECT id, warehouse_name FROM tbl_warehouse WHERE id = ANY($1)", ids)
                name_maps = {r["id"]: r["warehouse_name"] for r in rws}

        if deep == "customer_channel":
            ids = list(entity_map.keys())
            if ids:
                rws = await conn2.fetch("SELECT id, outlet_channel FROM outlet_channel WHERE id = ANY($1)", ids)
                name_maps = {r["id"]: r["outlet_channel"] for r in rws}

        if deep == "customer_category":
            ids = list(entity_map.keys())
            if ids:
                rws = await conn2.fetch("SELECT id, customer_category_name FROM customer_categories WHERE id = ANY($1)", ids)
                name_maps = {r["id"]: r["customer_category_name"] for r in rws}

        if deep == "customer":
            ids = list(entity_map.keys())
            if ids:
                rws = await conn2.fetch("SELECT id, name FROM agent_customers WHERE id = ANY($1)", ids)
                name_maps = {r["id"]: r["name"] for r in rws}

        if deep == "route_salesman":
            name_maps["route"] = {}
            name_maps["salesman"] = {}
            route_ids = [k[1] for k in entity_map.keys() if k[0] == "route"]
            if route_ids:
                rws = await conn2.fetch("SELECT id, route_name FROM tbl_route WHERE id = ANY($1)", route_ids)
                name_maps["route"] = {r["id"]: r["route_name"] for r in rws}
            sales_ids = [k[1] for k in entity_map.keys() if k[0] == "salesman"]
            if sales_ids:
                rws = await conn2.fetch("SELECT id, name FROM salesman WHERE id = ANY($1)", sales_ids)
                name_maps["salesman"] = {r["id"]: r["name"] for r in rws}
    finally:
        await conn2.close()
   

        # ------------------------------------------------------------
        # EXTRA NAME MAP LOADING FOR DEFAULT VIEW (VERY IMPORTANT)
        # ------------------------------------------------------------
        conn3 = await asyncpg.connect(DB_URL)
        try:
            # company
            if payload.company_ids:
                rws = await conn3.fetch(
                    "SELECT id, company_name FROM tbl_company WHERE id = ANY($1)",
                    payload.company_ids
                )
                name_maps["company"] = {r["id"]: r["company_name"] for r in rws}

            # region
            if payload.region_ids:
                rws = await conn3.fetch(
                    "SELECT id, region_name FROM tbl_region WHERE id = ANY($1)",
                    payload.region_ids
                )
                name_maps["region"] = {r["id"]: r["region_name"] for r in rws}

            # area
            if payload.area_ids:
                rws = await conn3.fetch(
                    "SELECT id, area_name FROM tbl_areas WHERE id = ANY($1)",
                    payload.area_ids
                )
                name_maps["area"] = {r["id"]: r["area_name"] for r in rws}

            # warehouse
            if payload.warehouse_ids:
                rws = await conn3.fetch(
                    "SELECT id, warehouse_name FROM tbl_warehouse WHERE id = ANY($1)",
                    payload.warehouse_ids
                )
                name_maps["warehouse"] = {r["id"]: r["warehouse_name"] for r in rws}

            # route
            if payload.route_ids:
                rws = await conn3.fetch(
                    "SELECT id, route_name FROM tbl_route WHERE id = ANY($1)",
                    payload.route_ids
                )
                name_maps["route"] = {r["id"]: r["route_name"] for r in rws}

            # salesman
            if payload.salesman_ids:
                rws = await conn3.fetch(
                    "SELECT id, name FROM salesman WHERE id = ANY($1)",
                    payload.salesman_ids
                )
                name_maps["salesman"] = {r["id"]: r["name"] for r in rws}

            # customer channel
            if payload.customer_channel_ids:
                rws = await conn3.fetch(
                    "SELECT id, outlet_channel FROM outlet_channel WHERE id = ANY($1)",
                    payload.customer_channel_ids
                )
                name_maps["customer_channel"] = {r["id"]: r["outlet_channel"] for r in rws}

            # customer category
            if payload.customer_category_ids:
                rws = await conn3.fetch(
                    "SELECT id, customer_category_name FROM customer_categories WHERE id = ANY($1)",
                    payload.customer_category_ids
                )
                name_maps["customer_category"] = {r["id"]: r["customer_category_name"] for r in rws}

            # customer
            if payload.customer_ids:
                rws = await conn3.fetch(
                    "SELECT id, name FROM agent_customers WHERE id = ANY($1)",
                    payload.customer_ids
                )
                name_maps["customer"] = {r["id"]: r["name"] for r in rws}

        finally:
            await conn3.close()

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})

    header_fmt = workbook.add_format({"bold": True, "bg_color": "#337a2c", "align": "center","font_color": "white"})
    number_fmt = workbook.add_format({"num_format": "#,##0.00"})
    cat_label_fmt = workbook.add_format({"bold": True, "font_color": "green"})
    bold_green = workbook.add_format({"bold": True, "font_color": "green", "num_format": "#,##0.00"})
    grand_fmt = workbook.add_format({"bold": True, "font_color": "red", "num_format": "#,##0.00"})

# -------------------- SUMMARY SHEET HANDLING --------------------
    ws = workbook.add_worksheet("summary")

    if is_default_view:
        # -------------------------------------------------------
        # 1. Determine which entity level we are displaying
        # -------------------------------------------------------
        entity_type, entity_ids, entity_name_map = determine_default_entities(payload, name_maps)

        # Column Header = entity names
        entity_names = [entity_name_map.get(eid, str(eid)) for eid in entity_ids]

        # -------------------------------------------------------
        # 2. Build a pivot: item → entity → sum(value)
        # -------------------------------------------------------
        default_map = {}   # {(item_id, name, cat): {entity_id: total}}

        for r in rows:
            item_id = r["item_id"]
            display_name = f"{r['item_code']} - {r['item_name']}"
            cat = r["item_category_name"]

            key = (item_id, display_name, cat)

            val = float(r["total_value"] or 0.0)

            # find correct entity for this row
            if entity_type in ("customer", "customer_category", "customer_channel",
                            "salesman", "route", "warehouse", "area", "region", "company"):
                ent = r.get(entity_type + "_id")

            if ent in entity_ids:
                default_map.setdefault(key, {}).setdefault(ent, 0.0)
                default_map[key][ent] += val

        # -------------------------------------------------------
        # 3. Write Excel sheet
        # -------------------------------------------------------
        

        # Headers
        ws.write(0, 0, "Item ID", header_fmt)
        ws.write(0, 1, "Item Name", header_fmt)
        ws.write(0, 2, "Category", header_fmt)

        col = 3
        for nm in entity_names:
            ws.write(0, col, nm, header_fmt)
            col += 1

        ws.write(0, col, "Total", header_fmt)

        # -------------------------------------------------------
        # 4. Fill rows
        # -------------------------------------------------------
        row = 1
        category_totals = {}  # category → total
        grand_total = 0

        for (item_id, name, cat), ent_map in default_map.items():
            ws.write(row, 0, item_id)
            ws.write(row, 1, name)
            ws.write(row, 2, cat)

            total_val = 0
            col = 3

            # one column per entity
            for eid in entity_ids:
                v = ent_map.get(eid, 0.0)
                ws.write(row, col, v, number_fmt)
                total_val += v
                col += 1

            # write total
            ws.write(row, col, total_val, bold_green)

            # accumulate category totals
            category_totals.setdefault(cat, 0.0)
            category_totals[cat] += total_val
            grand_total += total_val

            row += 1


        # -------------------------------------------------------
        # 5. CATEGORY TOTALS (formatted like your screenshot)
        # -------------------------------------------------------

        row += 1  # extra spacing

        ws.write(row, 1, "Category", header_fmt)

        col = 3
        for nm in entity_names:
            ws.write(row, col, nm, header_fmt)
            col += 1

        ws.write(row, col, "Total", header_fmt)
        row += 1

        # Compute per-entity totals and per-category totals
        entity_totals = {eid: 0 for eid in entity_ids}
        grand_total = 0

        for cat in sorted(category_totals.keys()):
            ws.write(row, 1, cat, cat_label_fmt)

            col = 3
            cat_total = 0
            for eid in entity_ids:
                # Sum only for this category
                value = 0
                for (item_id, name, c), ent_map in default_map.items():
                    if c == cat:
                        value += ent_map.get(eid, 0.0)

                ws.write(row, col, value, number_fmt)
                cat_total += value
                entity_totals[eid] += value
                col += 1

            ws.write(row, col, cat_total, number_fmt)
            grand_total += cat_total
            row += 1

        # -------------------------------------------------------
        # FINAL GRAND TOTAL ROW (RED BOLD LIKE YOUR SCREENSHOT)
        # -------------------------------------------------------
        ws.write(row, 1, "Total", grand_fmt)

        col = 3
        for eid in entity_ids:
            ws.write(row, col, entity_totals[eid], grand_fmt)
            col += 1

        ws.write(row, col, grand_total, grand_fmt)

        # -------------------------------------------------------
        # DONE — return file
        # -------------------------------------------------------
        workbook.close()
        output.seek(0)
        filename = f"Sales_Report_{payload.from_date}_to_{payload.to_date}.xlsx"
        return Response(
            content=output.read(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'}
        )
    # -------------------- NORMAL (NON-DEFAULT) BEHAVIOR --------------------
    write_aggregated_sheet(
        ws,
        summary_map,
        sorted_periods,
        header_fmt,
        number_fmt,
        cat_label_fmt,
        bold_green,
        grand_fmt
    )

    used_names = set(workbook.sheetnames)

    def _sheet(name):
        safe = _safe_sheet_name(name)
        base = safe
        i = 2
        while safe.lower() in used_names:
            safe = f"{base} ({i})"
            i += 1
        used_names.add(safe.lower())
        return safe

    def _add(map_data, name_map):
        for ent_id, items in map_data.items():
            nm = name_map.get(ent_id, str(ent_id))
            ws2 = workbook.add_worksheet(_sheet(nm))
            write_aggregated_sheet(
                ws2,
                items,
                sorted_periods,
                header_fmt,
                number_fmt,
                cat_label_fmt,
                bold_green,
                grand_fmt
            )

    if deep == "company":
        for cid in payload.company_ids:
            sheet_name = name_maps.get(cid, f"Company {cid}")
            safe_name = _sheet(sheet_name)
            comp_map = {}
            for r in rows:
                if r.get("company_id") == cid:
                    display_item = f'{r["item_code"]} - {r["item_name"]}'
                    key = (r["item_id"], display_item, r["item_category_name"])
                    raw_period = r[period_column]
                    period_final = iso_week_to_range(raw_period) if dataview == "weekly" else str(raw_period)
                    comp_map.setdefault(key, {}).setdefault(period_final, 0.0)
                    comp_map[key][period_final] += float(r["total_value"] or 0.0)

            ws_company = workbook.add_worksheet(safe_name)
            write_aggregated_sheet(
                ws_company,
                comp_map,
                sorted_periods,
                header_fmt,
                number_fmt,
                cat_label_fmt,
                bold_green,
                grand_fmt
            )

    elif deep == "route_salesman":
        if payload.route_ids:
            rmap = {k[1]: v for k, v in entity_map.items() if k[0] == "route"}
            _add(rmap, name_maps.get("route", {}))
        if payload.salesman_ids:
            smap = {k[1]: v for k, v in entity_map.items() if k[0] == "salesman"}
            _add(smap, name_maps.get("salesman", {}))

    elif deep in ("region", "area", "warehouse", "customer_channel", "customer_category", "customer"):
        _add(entity_map, name_maps)

    workbook.close()
    output.seek(0)

    filename = f"Sales_Report_{payload.from_date}_to_{payload.to_date}.xlsx"
    return Response(
        content=output.read(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    )





