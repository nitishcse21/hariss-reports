from datetime import datetime
from sqlalchemy import text
from app.sales_report.schemas.sales_schema import FilterSelection
from app.sales_report.utils.sales_common_helper import quantity_expr_sql,build_query_parts,validate_mandatory


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



def prepare_dashboard_context(filters: FilterSelection):
    validate_mandatory(filters)

    granularity, period_label_sql, order_by_sql = choose_granularity(
        filters.from_date, filters.to_date
    )

    joins, where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)
    join_sql = "\n".join(joins)

    quantity = quantity_expr_sql()
    value_expr = (
        quantity if filters.search_type.lower() == "quantity"
        else "SUM(id.item_total)"
    )

    return {
        "granularity": granularity,
        "period_label_sql": period_label_sql,
        "order_by_sql": order_by_sql,
        "join_sql": join_sql,
        "where_sql": where_sql,
        "params": params,
        "value_expr": value_expr,
    }


def detect_level(filters: FilterSelection):
    if filters.warehouse_ids:
        return "warehouse"
    elif filters.area_ids:
        return "area"
    elif filters.region_ids:
        return "region"
    return "company"


def get_top_tables(
    conn,
    *,
    value_expr: str,
    where_sql: str,
    params: dict,
    join_sql_base: str,
    join_sql_ws: str,
    limit: int = 10,
):
    out = {}

    # Top Salesmen
    sql = f"""
        SELECT
            s.osa_code || ' - ' || s.name AS salesman,
            w.warehouse_name,
            {value_expr} AS value
        FROM invoice_headers ih
        JOIN invoice_details id ON id.header_id = ih.id
        JOIN salesman s ON s.id = ih.salesman_id
        LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
        ) iu ON iu.item_id = id.item_id
        {join_sql_ws}
        WHERE {where_sql}
        GROUP BY s.osa_code, s.name, w.warehouse_name
        ORDER BY value DESC
        LIMIT {limit}
    """
    out["top_salesmen"] = [
        dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
    ]

    #  Top Warehouses
    sql = f"""
        SELECT
            w.warehouse_code || ' - ' || w.warehouse_name AS warehouse_name,
            w.address AS location,
            {value_expr} AS value
        FROM invoice_headers ih
        JOIN invoice_details id ON id.header_id = ih.id
        LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
        ) iu ON iu.item_id = id.item_id
        {join_sql_ws}
        WHERE {where_sql}
        GROUP BY w.warehouse_code, w.warehouse_name, w.address
        ORDER BY value DESC
        LIMIT {limit}
    """
    out["top_warehouses"] = [
        dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
    ]

    #  Top Items
    sql = f"""
        SELECT
            it.name AS item_name,
            {value_expr} AS value
        FROM invoice_headers ih
        JOIN invoice_details id ON id.header_id = ih.id
        JOIN items it ON it.id = id.item_id
        LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
        ) iu ON iu.item_id = id.item_id
        {join_sql_base}
        WHERE {where_sql}
        GROUP BY it.name
        ORDER BY value DESC
        LIMIT {limit}
    """
    out["top_items"] = [
        dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
    ]

    # Top Customers
    sql = f"""
        SELECT
            cst.id AS customer_id,
            cst.osa_code || ' - ' || cst.name AS customer_name,
            cst.contact_no AS contact,
            w.warehouse_name,
            {value_expr} AS value
        FROM invoice_headers ih
        JOIN invoice_details id ON id.header_id = ih.id
        JOIN agent_customers cst ON cst.id = ih.customer_id
        LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
        ) iu ON iu.item_id = id.item_id
        {join_sql_ws}
        WHERE {where_sql}
        GROUP BY
            cst.id,
            cst.osa_code,
            cst.name,
            cst.contact_no,
            w.warehouse_name
        ORDER BY value DESC
        LIMIT {limit}
    """
    out["top_customers"] = [
        dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()
    ]

    return out

