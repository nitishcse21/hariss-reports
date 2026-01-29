from fastapi import APIRouter
from app.primary_order_report.schemas.pmry_ord_schema import PrimaryOrderReportSchema
from sqlalchemy.sql import text
from app.database import engine
from app.primary_order_report.utils.pmry_ord_common_helper import (
    validate_mandatory,
    choose_granularity,
    build_query_parts,
)


router = APIRouter()


@router.post("/pmry-ord-dashboard")
def pmry_ord_dashboard(filters: PrimaryOrderReportSchema):

    validate_mandatory(filters)

    granularity, period_label_sql, order_by_sql = choose_granularity(
        filters.from_date, filters.to_date
    )

    joins, where_fragments, params = build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)
    join_sql = "\n".join(joins)

    if filters.warehouse_ids:
        level = "warehouse"
    elif filters.area_ids:
        level = "area"
    elif filters.region_ids:
        level = "region"
    else:
        level = "company"

    out = {
        "level": level,
        "granularity": granularity,
        "kpis": {},
        "trend_line": {},
    }
    with engine.connect() as conn:

        sql = f"""
            SELECT
                COUNT(*) AS total_orders,
                
                COUNT(*) FILTER (
                    WHERE
                        (
                            (hth.sap_id IS NULL AND hth.sap_msg IS NULL)
                        )
                ) AS order_pending,

                COUNT(*) FILTER (
                    WHERE (htd.status = 0)
                ) AS delivery_pending

            FROM ht_po_order_header hth
            LEFT JOIN ht_delivery_header htd 
            ON htd.order_id = hth.id
            {join_sql}
            WHERE {where_sql}
        """
        
        row = conn.execute(text(sql), params).mappings().first()
        out["kpis"] = {
            "total_orders": row["total_orders"],
            "order_pending": row["order_pending"],
            "delivery_pending": row["delivery_pending"],
        }

        sql = f"""
            SELECT
                {period_label_sql} AS period,
                COUNT(*) AS total_orders,

                COUNT(*) FILTER (
                    WHERE
                        (
                            (hth.sap_id IS NULL AND hth.sap_msg IS NULL)
                        )
                ) AS order_pending,

                COUNT(*) FILTER (
                    WHERE                   
                    (htd.status = 0)             
                ) AS delivery_pending

            FROM ht_po_order_header hth
            LEFT JOIN ht_delivery_header htd 
            ON htd.order_id = hth.id
            {join_sql}
            WHERE {where_sql}
            GROUP BY {order_by_sql}
            ORDER BY {order_by_sql}
        """

        rows = conn.execute(text(sql), params).mappings().all()
        out["trend_line"] = {"orders_over_time": rows}

    return {
        "data": out,
    }
