
from fastapi import APIRouter
from sqlalchemy import text
from app.database import engine
from app.load_unload_report.schemas.load_unload_schema import LoadUnloadReportRequest
from app.load_unload_report.utils.load_unload_common_helper import choose_granularity
from datetime import datetime

router = APIRouter()

@router.post("/load-unload-dashboard")
def load_unload_dashboard(payload: LoadUnloadReportRequest):

    # parse dates (if coming as strings)
    from_date = datetime.fromisoformat(payload.from_date).date()
    to_date   = datetime.fromisoformat(payload.to_date).date()

    granularity, period_sql, order_sql = choose_granularity(
        from_date, to_date, "h.created_at"
    )
    days = (to_date - from_date).days + 1

    # Condition 2: must select warehouse or salesman
    if not payload.warehouse_id and not payload.salesman_id:
        return {
            "error": "Please select either warehouse or salesman"
        }

    # ---------------- KPI SQL ----------------
    KPI_SQL = """
    SELECT
        ROUND(SUM(COALESCE(l.load_qty,0))::numeric, 3)   AS total_load,
        ROUND(SUM(COALESCE(u.unload_qty,0))::numeric, 3) AS total_unload,
        ROUND(SUM(COALESCE(s.sales_qty,0))::numeric, 3)  AS total_sales
    FROM items i

    LEFT JOIN (
        SELECT d.item_id,
        ROUND(SUM(
            CASE WHEN d.uom IN (1,3)
                THEN d.qty / NULLIF(iu.upc::numeric,0)
                ELSE d.qty END
        )::numeric,3) AS load_qty
        FROM tbl_load_header h
        JOIN tbl_load_details d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        WHERE h.created_at BETWEEN :from_date AND :to_date
        AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
        AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY d.item_id
    ) l ON l.item_id = i.id

    LEFT JOIN (
        SELECT d.item_id,
        ROUND(SUM(
            CASE WHEN d.uom IN (1,3)
                THEN d.qty / NULLIF(iu.upc::numeric,0)
                ELSE d.qty END
        )::numeric,3) AS unload_qty
        FROM tbl_unload_header h
        JOIN tbl_unload_detail d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        WHERE h.unload_date BETWEEN :from_date AND :to_date
        AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
        AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY d.item_id
    ) u ON u.item_id = i.id

    LEFT JOIN (
        SELECT d.item_id,
        ROUND(SUM(
            CASE WHEN d.uom IN (1,3)
                THEN d.quantity / NULLIF(iu.upc::numeric,0)
                ELSE d.quantity END
        )::numeric,3) AS sales_qty
        FROM invoice_headers h
        JOIN invoice_details d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        WHERE h.invoice_date BETWEEN :from_date AND :to_date
        AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
        AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY d.item_id
    ) s ON s.item_id = i.id

    WHERE COALESCE(l.load_qty,0) <> 0
    OR COALESCE(u.unload_qty,0) <> 0
    OR COALESCE(s.sales_qty,0) <> 0;

    """

    # ---------------- Salesman Summary ----------------

    SALESMAN_SQL = """
    SELECT
        salesman_id,
        salesman_name,
        ROUND(SUM(load)::numeric,3)   AS load,
        ROUND(SUM(unload)::numeric,3) AS unload,
        ROUND(SUM(sales)::numeric,3)  AS sales
    FROM (

        -- LOAD
        SELECT
            h.salesman_id,
            sm.name AS salesman_name,
            SUM(
                CASE WHEN d.uom IN (1,3)
                    THEN d.qty / NULLIF(iu.upc::numeric,0)
                    ELSE d.qty
                END
            ) AS load,
            0 AS unload,
            0 AS sales
        FROM tbl_load_header h
        JOIN tbl_load_details d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        LEFT JOIN salesman sm ON sm.id = h.salesman_id
        WHERE h.created_at BETWEEN :from_date AND :to_date
        AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
        AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY h.salesman_id, sm.name

        UNION ALL

        -- UNLOAD
        SELECT
            h.salesman_id,
            sm.name AS salesman_name,
            0 AS load,
            SUM(
                CASE WHEN d.uom IN (1,3)
                    THEN d.qty / NULLIF(iu.upc::numeric,0)
                    ELSE d.qty
                END
            ) AS unload,
            0 AS sales
        FROM tbl_unload_header h
        JOIN tbl_unload_detail d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        LEFT JOIN salesman sm ON sm.id = h.salesman_id
        WHERE h.unload_date BETWEEN :from_date AND :to_date
        AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
        AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY h.salesman_id, sm.name

        UNION ALL

        -- SALES
        SELECT
            h.salesman_id,
            sm.name AS salesman_name,
            0 AS load,
            0 AS unload,
            SUM(
                CASE WHEN d.uom IN (1,3)
                    THEN d.quantity / NULLIF(iu.upc::numeric,0)
                    ELSE d.quantity
                END
            ) AS sales
        FROM invoice_headers h
        JOIN invoice_details d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        LEFT JOIN salesman sm ON sm.id = h.salesman_id
        WHERE h.invoice_date BETWEEN :from_date AND :to_date
        AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
        AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY h.salesman_id, sm.name

    ) t
    GROUP BY salesman_id, salesman_name
    HAVING
        SUM(load)   <> 0
        OR SUM(unload) <> 0
        OR SUM(sales)  <> 0
    ORDER BY salesman_name
    """


    # ---------------- Trend (LOAD example) ----------------

    TREND_SQL = f"""
    SELECT
        period,
        ROUND(SUM(load)::numeric,3)   AS load,
        ROUND(SUM(unload)::numeric,3) AS unload,
        ROUND(SUM(sales)::numeric,3)  AS sales
    FROM (

        -- LOAD
        SELECT
            {period_sql.replace("h.created_at", "h.created_at")} AS period,
            SUM(
                CASE WHEN d.uom IN (1,3)
                    THEN d.qty / NULLIF(iu.upc::numeric,0)
                    ELSE d.qty
                END
            ) AS load,
            0 AS unload,
            0 AS sales
        FROM tbl_load_header h
        JOIN tbl_load_details d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        WHERE h.created_at BETWEEN :from_date AND :to_date
        AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
        AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY period

        UNION ALL

        -- UNLOAD
        SELECT
            {period_sql.replace("h.created_at", "h.unload_date")} AS period,
            0 AS load,
            SUM(
                CASE WHEN d.uom IN (1,3)
                    THEN d.qty / NULLIF(iu.upc::numeric,0)
                    ELSE d.qty
                END
            ) AS unload,
            0 AS sales
        FROM tbl_unload_header h
        JOIN tbl_unload_detail d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        WHERE h.unload_date BETWEEN :from_date AND :to_date
        AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
        AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY period

        UNION ALL

        -- SALES
        SELECT
            {period_sql.replace("h.created_at", "h.invoice_date")} AS period,
            0 AS load,
            0 AS unload,
            SUM(
                CASE WHEN d.uom IN (1,3)
                    THEN d.quantity / NULLIF(iu.upc::numeric,0)
                    ELSE d.quantity
                END
            ) AS sales
        FROM invoice_headers h
        JOIN invoice_details d ON h.id = d.header_id
        LEFT JOIN item_uoms iu ON iu.item_id = d.item_id
        WHERE h.invoice_date BETWEEN :from_date AND :to_date
        AND (:warehouse_id IS NULL OR h.warehouse_id = :warehouse_id)
        AND (:salesman_id IS NULL OR h.salesman_id = :salesman_id)
        GROUP BY period

    ) t
    GROUP BY period
    HAVING
        SUM(load)   <> 0
        OR SUM(unload) <> 0
        OR SUM(sales)  <> 0
    ORDER BY period
    """


    params = payload.dict()
    params["warehouse_id"] = payload.warehouse_id or None
    params["salesman_id"] = payload.salesman_id or None


    with engine.connect() as conn:
        kpi = conn.execute(text(KPI_SQL), params).mappings().first()
        salesman = conn.execute(text(SALESMAN_SQL), params).mappings().all()
        if days > 1:
            trend = conn.execute(text(TREND_SQL), params).mappings().all()
        else:
            trend = []


    return {
        "kpi": kpi,
        "salesman_summary": salesman,
        "trend": trend,
        "granularity": granularity
    }
