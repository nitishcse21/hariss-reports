from fastapi import HTTPException
from app.customer_dashboard.schemas.cust_dash_schama import CustDashboardRequest 
from datetime import datetime
from sqlalchemy import text
from app.database import engine



def validate_mandatory(filters: CustDashboardRequest):
    if not filters.from_date or not filters.to_date:
        raise HTTPException(status_code=400, detail="from_date and to_date are required")
    try:
        _ = datetime.fromisoformat(filters.from_date)
        _ = datetime.fromisoformat(filters.to_date)
    except Exception:
        raise HTTPException(status_code=400, detail="from_date/to_date must be in YYYY-MM-DD format")



def build_query_parts(filters:CustDashboardRequest):
    where_fragments = []
    params = {}

    where_fragments.append("ih.invoice_date BETWEEN :from_date AND :to_date")
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.display_quantity and filters.display_quantity.lower() == "without_free_good":
        where_fragments.append("id.item_total <> 0")

    return where_fragments, params


def new_customer_date(filters:CustDashboardRequest):
    where_date = []
    params = {}

    where_date.append("ac.created_at BETWEEN :from_date AND :to_date")
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    return where_date, params


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


def quantity_expr_sql():
    return """
    ROUND(
        CAST(
            SUM(
                CASE
                    WHEN id.uom IN (1,3)
                         AND NULLIF(iu.upc::numeric, 0) IS NOT NULL
                    THEN id.quantity / iu.upc::numeric
                    ELSE id.quantity
                END
            ) AS numeric
        ),
        3
    )
    """



def exchange_expr_sql():
    return """
    ROUND(
        CAST(
            SUM(
                CASE
                    WHEN ei.uom_id IN (1,3)
                         AND NULLIF(iu.upc::numeric, 0) IS NOT NULL
                    THEN ei.item_quantity / iu.upc::numeric
                    ELSE ei.item_quantity
                END
            ) AS numeric
        ),
        3
    )
    """


def get_top_customers_dashboard(where_sql: str, params: dict):

    quantity = quantity_expr_sql()
    exchange = exchange_expr_sql()

    query = f"""
    WITH uom_data AS (
        SELECT 
            item_id, 
            MAX(NULLIF(upc::numeric, 0)) AS upc
        FROM item_uoms
        GROUP BY item_id
    ),

    invoice_agg AS (
        SELECT
            ih.customer_id,
            {quantity} AS total_qty
        FROM invoice_headers ih
        JOIN invoice_details id ON id.header_id = ih.id
        LEFT JOIN uom_data iu ON iu.item_id = id.item_id
        WHERE {where_sql}
        GROUP BY ih.customer_id
    ),

    exchange_agg AS (
        SELECT
            eh.customer_id,
            {exchange} AS exchange_qty
        FROM exchange_headers eh
        JOIN exchange_in_invoices ei ON ei.header_id = eh.id
        LEFT JOIN uom_data iu ON iu.item_id = ei.item_id
        WHERE eh.created_at BETWEEN :from_date AND :to_date
        GROUP BY eh.customer_id
    )

    SELECT
        ac.name AS customer_name,
        oc.outlet_channel AS outlet_channel_name,
        cc.customer_category_name AS customer_category_name,
        COALESCE(inv.total_qty, 0) AS value,
        COALESCE(ex.exchange_qty, 0) AS exchange_qty

    FROM invoice_agg inv
    JOIN agent_customers ac ON ac.id = inv.customer_id
    JOIN outlet_channel oc ON oc.id = ac.outlet_channel_id
    JOIN customer_categories cc ON cc.id = ac.category_id
    LEFT JOIN exchange_agg ex ON ex.customer_id = inv.customer_id

    ORDER BY value DESC
    LIMIT 100
    """

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return rows or []
