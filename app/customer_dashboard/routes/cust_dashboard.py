from fastapi import APIRouter
from sqlalchemy import text
from app.customer_dashboard.schemas.cust_dash_schama import CustDashboardRequest
from app.customer_dashboard.utils.cust_dash_helper import (
    validate_mandatory,
    choose_granularity,
    build_query_parts,
    quantity_expr_sql,
    new_customer_date,
    get_top_customers_dashboard,
)
from app.database import engine

router = APIRouter(tags=["Customer Dashboard"])
quantity = quantity_expr_sql()


@router.post("/cust-dashboard-kpis")
def cust_dashboard_kpis(filters: CustDashboardRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    where_date, params = new_customer_date(filters)
    created_at = " AND ".join(where_date)

    out = {"kpis": {}}

    with engine.connect() as conn:
        # KPIS
        query = f"""
                SELECT COUNT(DISTINCT ih.customer_id) as total_customer
                FROM invoice_headers ih
                JOIN agent_customers ac ON ac.id = ih.customer_id
                WHERE {where_sql}             
            """
        rows = conn.execute(text(query), params).scalar()
        out["kpis"]["total_customer"] = rows

        query = """
                SELECT COUNT(DISTINCT nc.id) total_pending
                FROM new_customer nc
                WHERE nc.approval_status = 2
                """
        rows = conn.execute(text(query), params).scalar()
        out["kpis"]["total_pending"] = rows

        query = f"""
                SELECT COUNT(DISTINCT ac.id) total_new_customer
                FROM agent_customers ac
                WHERE {created_at}
                """
        rows = conn.execute(text(query), params).scalar()
        out["kpis"]["total_new_customer"] = rows

    return out


@router.post("/cust-dashboard-trend")
def cust_dashboard_trend(filters: CustDashboardRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    granularity, period_label_sql, order_by_sql = choose_granularity(
        filters.from_date, filters.to_date
    )

    out = {
        "granularity":granularity
       }

    with engine.connect() as conn:
        
        query = f"""
                SELECT
                {period_label_sql} as period_label,
                {quantity} as value              
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                LEFT JOIN (
                SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                FROM item_uoms
                GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                WHERE {where_sql}
                GROUP BY {period_label_sql},{order_by_sql}
                ORDER BY {order_by_sql}
                """
        rows = conn.execute(text(query), params).fetchall()
        out["trend-line"] = [dict(r._mapping) for r in rows]

        
        query = f"""
                SELECT
                {period_label_sql} as period_label,
                ac.name as customer_name,
                {quantity} as value              
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                JOIN agent_customers ac ON ac.id = ih.customer_id
                LEFT JOIN (
                SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                FROM item_uoms
                GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                WHERE {where_sql}
                GROUP BY {period_label_sql},{order_by_sql}, ac.name
                ORDER BY {quantity} DESC
                LIMIT 20
                """
        rows = conn.execute(text(query), params).fetchall()
        out["top_cust_trend-line"] = [dict(r._mapping) for r in rows]

    return out



@router.post("/cust-dash-region")
def cust_dash_region(filters: CustDashboardRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    out = {}

    with engine.connect() as conn:

        query = f"""
            SELECT
            r.region_name AS region_name,
            COUNT(DISTINCT ih.customer_id) AS total_customers,
            ROUND(
            COUNT(DISTINCT ih.customer_id) * 100.0
            / SUM(COUNT(DISTINCT ih.customer_id)) OVER (),2) 
            AS percentage
            FROM invoice_headers ih
            JOIN agent_customers ac ON ac.id = ih.customer_id
            JOIN tbl_warehouse w ON w.id = ih.warehouse_id
            JOIN tbl_region r ON r.id = w.region_id
            WHERE {where_sql}
            GROUP BY r.region_name
            ORDER BY total_customers DESC;
            """
        rows = conn.execute(text(query), params).fetchall()
        out["customer_by_region"] = [dict(r._mapping) for r in rows]

    return out



@router.post("/cust-dash-area")
def cust_dash_area(filters: CustDashboardRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    out = {}

    with engine.connect() as conn:

        query = f"""
            SELECT
            a.area_code || '-' || a.area_name as area_name,
            COUNT(DISTINCT ih.customer_id) AS total_customers,
            ROUND(
            COUNT(DISTINCT ih.customer_id) * 100.0
            / SUM(COUNT(DISTINCT ih.customer_id)) OVER (),2) 
            AS percentage
            FROM invoice_headers ih
            JOIN agent_customers ac ON ac.id = ih.customer_id
            JOIN tbl_warehouse w ON w.id = ih.warehouse_id
            JOIN tbl_areas a ON a.id = w.area_id
            WHERE {where_sql}
            GROUP BY a.area_name, a.area_code
            ORDER BY total_customers DESC;
            """
        rows = conn.execute(text(query), params).fetchall()
        out["customer_by_area"] =  [dict(r._mapping) for r in rows]

    return out


@router.post("/cust-dash-channel")
def cust_dash_channel(filters:CustDashboardRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    out = {}

    with engine.connect() as conn:
        query = f"""
            SELECT
            oc.outlet_channel AS outlet_channel_name,
            COUNT(DISTINCT ih.customer_id) AS total_customers,
            ROUND(
            COUNT(DISTINCT ih.customer_id) * 100.0
            / SUM(COUNT(DISTINCT ih.customer_id)) OVER (),2) 
            AS percentage
            FROM invoice_headers ih
            JOIN agent_customers ac ON ac.id = ih.customer_id
            JOIN outlet_channel oc ON oc.id = ac.outlet_channel_id
            WHERE {where_sql}
            GROUP BY oc.outlet_channel
            ORDER BY total_customers DESC;
            """
        rows = conn.execute(text(query), params).fetchall()
        out["customer_by_channel"] =  [dict(r._mapping) for r in rows]

    return out



@router.post("/cust-dash-category")
def cust_dash_category(filters:CustDashboardRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    out = {}

    with engine.connect() as conn:
        query = f"""
            SELECT
            cc.customer_category_name AS customer_category_name,
            COUNT(DISTINCT ih.customer_id) AS total_customers,
            ROUND(
            COUNT(DISTINCT ih.customer_id) * 100.0
            / SUM(COUNT(DISTINCT ih.customer_id)) OVER (),2) 
            AS percentage
            FROM invoice_headers ih
            JOIN agent_customers ac ON ac.id = ih.customer_id
            JOIN customer_categories cc ON cc.id = ac.category_id
            WHERE {where_sql}
            GROUP BY cc.customer_category_name
            ORDER BY total_customers DESC;
            """
        rows = conn.execute(text(query), params).fetchall()
        out["customer_by_category"] =  [dict(r._mapping) for r in rows]

    return out


@router.post("/cust-dash-top-customers")
def cust_dash_top_customers(filters:CustDashboardRequest):
    validate_mandatory(filters)

    where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    out = {}

    where_sql = """
        ih.invoice_date BETWEEN :from_date AND :to_date
    """
    params = {
        "from_date": filters.from_date,
        "to_date": filters.to_date
    }

    top_customers = get_top_customers_dashboard(where_sql, params)
    out["top_100_customers"] = top_customers

    return out
        