from fastapi import APIRouter
from sqlalchemy import text
from app.database import engine
from app.visit_report.schemas.visit_schema import VisitSchema
from app.visit_report.utils.visit_common_helper import validate_mandatory, choose_granularity, build_query_parts, build_customer_filter_parts


router = APIRouter()
@router.post("/visit-dashboard")
def visit_dashboard(filters: VisitSchema):
    validate_mandatory(filters)

    granularity, period_label_sql, order_by_sql = choose_granularity(
        filters.from_date, filters.to_date
    )

    joins, where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)
    join_sql_base = "\n".join(joins)

    customer_where_sql, customer_params = build_customer_filter_parts(filters)

    if filters.salesman_ids:
        level = "salesman"
    elif filters.route_ids:
        level = "route"
    else:
        level = "warehouse"

    out = {
        "level": level,
        "granularity": granularity,
        "kpis": {},
        "trend-line": {},
        "table" : {},
    }

    with engine.connect() as conn:

        sql = f"""
            SELECT
                COUNT(vp.id) AS total_visits_customers
            FROM
                visit_plan vp
            {join_sql_base}
            WHERE
                {where_sql}
        """
       
        row = conn.execute(text(sql), params).mappings().one()
        out["kpis"]["total_visits_customers"] = row["total_visits_customers"]


        sql = f"""
           SELECT COUNT(DISTINCT ac.id) AS total_customers
            FROM agent_customers ac
            JOIN tbl_salesman_warehouse_history sh ON sh.route_id = ac.route_id
            WHERE
                {customer_where_sql}
        """
        row = conn.execute(text(sql), customer_params).mappings().one()
        out["kpis"]["total_customers"] = row["total_customers"]


        sql = f"""
            SELECT
            COUNT(DISTINCT vp.id) FILTER (WHERE vp.shop_status = '1') AS total_open_shops,
            COUNT(DISTINCT vp.id) FILTER (WHERE vp.shop_status = '0') AS total_close_shops
            FROM
                visit_plan vp
            {join_sql_base}
            WHERE
                {where_sql}
            """
        row = conn.execute(text(sql), params).mappings().one()
        out["kpis"]["total_open_shops"] = row["total_open_shops"]
        out["kpis"]["total_close_shops"] = row["total_close_shops"]


        sql = f"""
            SELECT
                {period_label_sql} AS period_label,
                COUNT(*) FILTER (WHERE shop_status = '1')   AS open_shops,
                COUNT(*) FILTER (WHERE shop_status = '0') AS closed_shops
            FROM
                visit_plan vp
            {join_sql_base}
            WHERE
                {where_sql} 
            GROUP BY
                {order_by_sql}
            ORDER BY
                {order_by_sql} ASC
            """
        rows = conn.execute(text(sql), params).mappings().all()
        out["trend-line"]["data"] = list(rows)


        sql = f"""
            SELECT
                s.osa_code || '-' || s.name AS salesman_name,
                COUNT(vp.id) AS total_visits_customers,
                COUNT(*) FILTER (WHERE vp.shop_status = '1') AS open_shops,
                COUNT(*) FILTER (WHERE vp.shop_status = '0') AS closed_shops
            FROM
                visit_plan vp
            JOIN salesman s ON vp.salesman_id = s.id
            {join_sql_base}
            WHERE
                {where_sql}
            GROUP BY
                s.osa_code, s.name
            ORDER BY
                total_visits_customers DESC
            """
        rows = conn.execute(text(sql), params).mappings().all()
        out["table"]["data"] = list(rows)

    return out