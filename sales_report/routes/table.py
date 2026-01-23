from sales_report.schemas.sales_schema import FilterSelection
from sales_report.utils.common_helper import validate_mandatory, build_query_parts
from fastapi import APIRouter,Query,Request
from sqlalchemy import text
from database import engine
import time
from math import ceil



app = APIRouter()




@app.post("/sales-report-table")
def get_table(filters: FilterSelection, request:Request, page: int = Query(1, ge=1)):
    start = time.time()
    validate_mandatory(filters)

    joins, where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    # -----------------------
    # GROUPING PRIORITY
    # Always group by CUSTOMER if any customer filter applied
    # -----------------------
    if filters.item_ids:
        level_col = "it.name"
        level_name = "item_name"
        extra_joins = ""

    elif filters.item_category_ids:
        level_col = "cat.category_name"
        level_name = "item_category"
        extra_joins = ""

    elif (filters.customer_ids
        or filters.customer_channel_ids
        or filters.customer_category_ids):
        level_col = "c.name"
        level_name = "customer_name"
        extra_joins = """
            LEFT JOIN agent_customers c ON c.id = ih.customer_id
            LEFT JOIN customer_categories cc ON cc.id = c.category_id
            LEFT JOIN outlet_channel ch ON ch.id = c.outlet_channel_id
        """

    elif filters.salesman_ids:
        level_col = "sm.name"
        level_name = "salesman_name"
        extra_joins = "LEFT JOIN salesman sm ON sm.id = ih.salesman_id"

    elif filters.route_ids:
        level_col = "rt.route_name"
        level_name = "route_name"
        extra_joins = "LEFT JOIN tbl_route rt ON rt.id = ih.route_id"

    elif filters.warehouse_ids:
        level_col = "wh.warehouse_name"
        level_name = "warehouse_name"
        extra_joins = "LEFT JOIN tbl_warehouse wh ON wh.id = ih.warehouse_id"

    elif filters.area_ids:
        level_col = "ar.area_name"
        level_name = "area_name"
        extra_joins = """
            LEFT JOIN tbl_areas ar ON ar.id = w.area_id
        """

    elif filters.region_ids:
        level_col = "rg.region_name"
        level_name = "region_name"
        extra_joins = """
            LEFT JOIN tbl_areas ar ON ar.id = w.area_id
            LEFT JOIN tbl_region rg ON rg.id = ar.region_id
        """

    elif filters.company_ids:
        level_col = "co.company_name"
        level_name = "company_name"
        extra_joins = "LEFT JOIN tbl_company co ON co.id = ih.company_id"
    else:
        level_col = "co.company_name"
        level_name = "company_name"
        extra_joins = "LEFT JOIN tbl_company co ON co.id = ih.company_id"
        

    extra_join_lines = [j.strip() for j in extra_joins.split("\n") if j.strip()]
    for j in extra_join_lines:
        joins.append(j)
    joins = list(dict.fromkeys(joins))
    join_sql = "\n".join(joins)

    value_col = "SUM(id.quantity) AS total_quantity" \
        if filters.search_type.lower() == "quantity" \
        else "SUM(id.item_total) AS total_amount"

    select_fields = [
        "it.code AS item_code",
        "it.name AS item_name",
        "cat.category_name AS item_category",
        "ih.invoice_date",
        f"{level_col} AS {level_name}",
        value_col
    ]

    group_fields = [
        "it.code",
        "it.name",
        "cat.category_name",
        "ih.invoice_date",
        level_col
    ]

    # Add customer category only if filtered
    if filters.customer_category_ids:
        select_fields.insert(4, "cc.customer_category_name AS customer_category_name")
        group_fields.append("cc.customer_category_name")

    # Add channel only if filtered
    if filters.customer_channel_ids:
        select_fields.insert(4, "ch.outlet_channel AS channel_name")
        group_fields.append("ch.outlet_channel")


    select_sql = ",\n        ".join(select_fields)
    group_sql = ", ".join(group_fields)

    # Count total rows
    count_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT {level_col}
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN items it ON it.id = id.item_id
            LEFT JOIN item_categories cat ON cat.id = it.category_id
            {join_sql}
            WHERE {where_sql}
            GROUP BY {group_sql}
        ) AS subq
    """

    with engine.connect() as conn:
        total_rows = conn.execute(text(count_sql), params).scalar()

    rows_per_page = 50
    offset = (max(page,1) - 1) * rows_per_page
    
    total_pages = max(ceil(total_rows / rows_per_page),1)
    
    # Final data query
    final_sql = f"""
        SELECT
        {select_sql}
        FROM invoice_headers ih
        JOIN invoice_details id ON id.header_id = ih.id
        JOIN items it ON it.id = id.item_id
        LEFT JOIN item_categories cat ON cat.id = it.category_id
        {join_sql}
        WHERE {where_sql}
        GROUP BY {group_sql}
        ORDER BY ih.invoice_date, it.name
        LIMIT {rows_per_page} OFFSET {offset}
    """

    with engine.connect() as conn:
        rows = conn.execute(text(final_sql), params).fetchall()
    base_url = str(request.url).split("?")[0]
    end  = time.time()
    print(f"Query took {end - start} seconds")
    return {
        "total_rows": total_rows,
        
        "total_pages": total_pages,
        "current_page": page,
        "next_page": f"{base_url}?page={page + 1}" if page < total_pages else None,
        "previous_page": f"{base_url}?page={page - 1}" if page > 1 else None,
        "rows": [dict(r._mapping) for r in rows],
    }