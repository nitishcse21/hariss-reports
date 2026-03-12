from fastapi import APIRouter
from app.promotion_report.schemas.promotion_schema import PromotionRequest,PromotionDateRangeRequest
from app.promotion_report.utils.promotion_common_helper import (
    validate_mandatory,
    build_query_parts,
    choose_granularity,
)
from app.database import engine
from sqlalchemy import text

router = APIRouter(tags=["Promotion Dashboard"])


@router.post("/promotion-kpis")
def promotion_kpis(payload: PromotionRequest):
    validate_mandatory(payload)

    joins, where_fragments, params, status_case = build_query_parts(payload)
    where_sql = " AND ".join(where_fragments) if where_fragments else "1=1"
    join_sql = "\n".join(joins)

    out = {
        "kpis": {},
    }

    with engine.connect() as conn:
        kpi_sql = f"""
            SELECT
            COUNT(DISTINCT idl.promotion_id) AS total_running_promotions,
            COALESCE(SUM(idl.quantity),0) AS total_free_quantity,
            COALESCE(SUM(ih.total_amount),0) AS total_amount,
            COUNT(DISTINCT ih.customer_id) AS total_number_of_promotions_customers
            FROM invoice_details idl
            JOIN invoice_headers ih ON ih.id = idl.header_id
            {join_sql}
            WHERE {where_sql}
        """
        rows = conn.execute(text(kpi_sql), params).mappings().first()
        out["kpis"] = {
            "total_running_promotions": rows["total_running_promotions"],
            "total_free_quantity": rows["total_free_quantity"],
            "total_amount": rows["total_amount"],
            "total_number_of_promotions_customers": rows["total_number_of_promotions_customers"],
        }

    return out


@router.post("/promotion-trend-line")
def promotion_trend_line(payload: PromotionRequest):
    validate_mandatory(payload)

    joins, where_fragments, params, status_case = build_query_parts(payload)
    where_sql = " AND ".join(where_fragments) if where_fragments else "1=1"
    join_sql = "\n".join(joins)

    granularity, period_label_sql, order_by_sql = choose_granularity(
        payload.from_date, payload.to_date
    )

    out = {
        "granularity": granularity,
        "trend_line": {},
    }

    with engine.connect() as conn:
        trend_sql = f"""
            SELECT
            {period_label_sql} AS period,
            COUNT(DISTINCT idl.promotion_id) AS promotions_count
            FROM invoice_details idl
            JOIN invoice_headers ih ON ih.id = idl.header_id
            {join_sql}
            WHERE {where_sql}
            GROUP BY period,{order_by_sql}
            ORDER BY {order_by_sql}
        """
        rows = conn.execute(text(trend_sql), params).fetchall()
        out["trend_line"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/top-1000-customers")
def top_1000_cutomers(payload: PromotionDateRangeRequest):
    validate_mandatory(payload)

    with engine.connect() as conn:
        top_customers_sql = f"""
            SELECT
                w.warehouse_code || ' - ' || w.warehouse_name AS warehouse_name,
                ac.name AS customer_name,
                ac.contact_no,

                SUM(
                    CASE
                        WHEN idl.item_total = 0
                        THEN idl.quantity
                        ELSE 0
                    END
                ) AS total_free_quantity,

                SUM(
                    CASE
                        WHEN idl.item_total > 0
                        THEN idl.quantity
                        ELSE 0
                    END
                ) AS total_purchase_quantity,

                SUM(
                    CASE
                        WHEN idl.item_total > 0
                        THEN idl.item_total
                        ELSE 0
                    END
                ) AS total_purchase_amount,

                SUM(
                    CASE
                        WHEN idl.item_total = 0
                        THEN idl.quantity * idl.itemvalue
                        ELSE 0
                    END
                ) AS total_free_amount

                FROM invoice_details idl
                JOIN invoice_headers ih ON ih.id = idl.header_id
                JOIN agent_customers ac ON ac.id = ih.customer_id
                JOIN tbl_warehouse w ON w.id = ih.warehouse_id

                WHERE ih.invoice_date BETWEEN :from_date AND :to_date

                GROUP BY
                w.warehouse_code,
                w.warehouse_name,
                ac.name,
                ac.contact_no

                ORDER BY total_purchase_amount DESC
                LIMIT 1000
                """
        rows = conn.execute(
            text(top_customers_sql),
            {
                "from_date": payload.from_date,
                "to_date": payload.to_date
            }
        ).fetchall()

    return {"top_customers": [dict(r._mapping) for r in rows]}