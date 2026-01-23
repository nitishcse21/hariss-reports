from fastapi import APIRouter, HTTPException, Response
import asyncpg
import io
import xlsxwriter
from datetime import datetime
from database import DB_URL
from item_report.schemas.item_schema import FilterSelection
from item_report.utils.item_export_xlsx_helper import clip_period_to_range,choose_granularity, format_period_label, iso_week_to_range, get_deepest, sort_periods

app = APIRouter()


@app.post("/item-export")
async def export_item_quantity_report(payload: FilterSelection):

    # -------------------- DATE VALIDATION --------------------
    try:
        from_date = datetime.strptime(payload.from_date, "%Y-%m-%d").date()
        to_date = datetime.strptime(payload.to_date, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Dates must be YYYY-MM-DD")

    search_type = (payload.search_type or "quantity").lower()
    if search_type not in ("quantity", "amount"):
        raise HTTPException(status_code=400, detail="search_type must be quantity or amount")

    if not payload.company_ids:
        payload.company_ids = [1]

    # -------------------- DATAVIEW --------------------
    is_default_view = (
        payload.dataview is None or payload.dataview.strip().lower() == "default"
    )

    allowed = ["daily", "weekly", "monthly", "yearly"]
    if payload.dataview and payload.dataview.lower() in allowed:
        dataview = payload.dataview.lower()
    else:
        dataview = choose_granularity(from_date, to_date)

    period_column = {
        "daily": "daily_period",
        "weekly": "weekly_period",
        "monthly": "monthly_period",
        "yearly": "yearly_period",
    }[dataview]

    deep = get_deepest(payload)

    # -------------------- VALUE COLUMN --------------------
    if search_type == "quantity":
        value_col = """
            ROUND(
                CAST(
                    SUM(
                        CASE
                            WHEN ms.uom IN (1,3)
                                 AND iu.upc IS NOT NULL
                                 AND iu.upc > 0
                            THEN ms.total_quantity / iu.upc
                            ELSE ms.total_quantity
                        END
                    ) AS NUMERIC
                ), 3
            )
        """
    else:
        value_col = "SUM(ms.total_amount)"

    # -------------------- GROUPING --------------------
    if deep == "route":
        group_cols = [
            "ms.item_id", "ms.item_code", "ms.item_name",
            "ms.route_id", period_column
        ]
    elif deep == "warehouse":
        group_cols = [
            "ms.item_id", "ms.item_code", "ms.item_name",
            "ms.warehouse_id", period_column
        ]
    elif deep == "area":
        group_cols = [
            "ms.item_id", "ms.item_code", "ms.item_name",
            "ms.area_id", period_column
        ]
    elif deep == "region":
        group_cols = [
            "ms.item_id", "ms.item_code", "ms.item_name",
            "ms.region_id", period_column
        ]
    else:  # company
        group_cols = [
            "ms.company_id", "ms.item_id",
            "ms.item_code", "ms.item_name", period_column
        ]

    select_cols = ", ".join(group_cols)
    group_by = ", ".join(group_cols)

    # -------------------- WHERE CLAUSE --------------------
    where_parts = ["ms.invoice_date BETWEEN $1 AND $2"]
    params = [from_date, to_date]
    idx = 3

    def _add(col, vals):
        nonlocal idx
        if vals:
            where_parts.append(f"{col} = ANY(${idx})")
            params.append(vals)
            idx += 1

    _add("ms.company_id", payload.company_ids)
    _add("ms.region_id", payload.region_ids)
    _add("ms.area_id", payload.area_ids)
    _add("ms.warehouse_id", payload.warehouse_ids)
    _add("ms.route_id", payload.route_ids)
    _add("ms.item_id", payload.item_ids)

    # 🔹 ITEM BRAND FILTER
    if payload.brand_ids:
        where_parts.append(f"it.brand = ANY(${idx})")
        params.append(payload.brand_ids)
        idx += 1


    # 🔹 ITEM CATEGORY FILTER
    if payload.item_category_ids:
        where_parts.append(f"ms.item_category_id = ANY(${idx})")
        params.append(payload.item_category_ids)
        idx += 1

    # 🔹 FREE GOOD FILTER
    if search_type == "quantity" and payload.display_quantity == "without_free_good":
        where_parts.append("ms.total_amount > 0")

    where_clause = " AND ".join(where_parts)

    # -------------------- SQL --------------------
    sql = f"""
        SELECT
            {select_cols},
            {value_col} AS total_value
        FROM mv_sales_report_fast ms
        LEFT JOIN items it ON it.id = ms.item_id
        LEFT JOIN (
            SELECT item_id, MAX(upc::numeric) AS upc
            FROM item_uoms
            WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
            GROUP BY item_id
        ) iu ON iu.item_id = ms.item_id
        WHERE {where_clause}
        GROUP BY {group_by}
        ORDER BY ms.item_code, {period_column}
    """

    conn = await asyncpg.connect(DB_URL)
    rows = await conn.fetch(sql, *params)
    await conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    # -------------------- EXCEL GENERATION --------------------
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = workbook.add_worksheet("summary")

    header_fmt = workbook.add_format({
        "bold": True, "bg_color": "#337a2c",
        "font_color": "white", "align": "center"
    })
    number_fmt = workbook.add_format({"num_format": "#,##0.000"})
    grand_fmt = workbook.add_format({"bold": True, "font_color": "red"})

    # -------------------- DEFAULT VIEW --------------------
    if is_default_view:
        ws.write_row(0, 0, ["S.No", "Item Name", "Quantity"], header_fmt)

        totals = {}
        grand_total = 0.0

        for r in rows:
            name = f'{r["item_code"]} - {r["item_name"]}'
            val = float(r["total_value"] or 0.0)
            totals[name] = totals.get(name, 0.0) + val
            grand_total += val

        row = 1
        for i, (name, qty) in enumerate(sorted(totals.items()), start=1):
            ws.write(row, 0, int(i))                 # S.No → INTEGER
            ws.write(row, 1, name)                   # Item Name → TEXT
            ws.write_number(row, 2, qty, number_fmt) # Quantity → DECIMAL
            row += 1


        ws.write(row, 1, "Total", grand_fmt)
        ws.write(row, 2, grand_total, grand_fmt)

    else:
        # ---------- BUILD PIVOT ----------
        pivot = {}      # {item_name: {period: qty}}
        periods = set()

        for r in rows:
            item = f'{r["item_code"]} - {r["item_name"]}'
            raw_period = (
                iso_week_to_range(r[period_column])
                if dataview == "weekly"
                else str(r[period_column])
            )


            period = clip_period_to_range(
                raw_period,
                dataview,
                from_date,
                to_date
            )

            if period is None:
                continue

            qty = float(r["total_value"] or 0.0)

            pivot.setdefault(item, {})
            pivot[item][period] = pivot[item].get(period, 0.0) + qty
            periods.add(period)



        # ---------- SORT PERIODS ----------
        sorted_periods = sort_periods(periods, dataview)

        # ---------- HEADER ----------
        ws.write(0, 0, "S.No", header_fmt)
        ws.write(0, 1, "Item Name", header_fmt)

        col = 2
        for p in sorted_periods:
            ws.write(0, col, format_period_label(p, dataview), header_fmt)
            col += 1

        ws.write(0, col, "Total", header_fmt)

        # ---------- DATA ----------
        row = 1
        sno = 1
        grand_totals = {p: 0.0 for p in sorted_periods}
        grand_total = 0.0

        for item, per_map in pivot.items():
            ws.write(row, 0, sno)       # S.No → INT
            ws.write(row, 1, item)

            col = 2
            row_total = 0.0

            for p in sorted_periods:
                v = per_map.get(p, 0.0)
                ws.write_number(row, col, v, number_fmt)
                row_total += v
                grand_totals[p] += v
                col += 1

            ws.write_number(row, col, row_total, number_fmt)
            grand_total += row_total

            row += 1
            sno += 1

        # ---------- FINAL TOTAL ROW ----------
        ws.write(row, 1, "Total", grand_fmt)

        col = 2
        for p in sorted_periods:
            ws.write_number(row, col, grand_totals[p], grand_fmt)
            col += 1

        ws.write_number(row, col, grand_total, grand_fmt)


    workbook.close()
    output.seek(0)

    filename = f"Item_Quantity_Report_{payload.from_date}_to_{payload.to_date}.xlsx"
    return Response(
        content=output.read(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
