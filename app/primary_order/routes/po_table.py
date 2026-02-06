from app.primary_order.schemas.po_schemas import OrderSummaryFilters
from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import text
from app.database import engine
import math

router = APIRouter()


@router.post("/po-order-table")
def order_summary(
    request: Request,
    filters: OrderSummaryFilters,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, le=200)
):

    where_clauses = []
    params = {}

    # 🔹 Date filter (mandatory)
    where_clauses.append(
        "hth.order_date BETWEEN :from_date AND :to_date"
    )
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    # 🔹 Company
    if filters.company_ids:
        where_clauses.append("hth.company_id = ANY(:company_ids)")
        params["company_ids"] = filters.company_ids

    # 🔹 Region
    if filters.region_ids:
        where_clauses.append("tr.id = ANY(:region_ids)")
        params["region_ids"] = filters.region_ids

    # 🔹 Area
    if filters.area_ids:
        where_clauses.append("ta.id = ANY(:area_ids)")
        params["area_ids"] = filters.area_ids

    # 🔹 Warehouse
    if filters.warehouse_ids:
        where_clauses.append("tw.id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = filters.warehouse_ids

    where_sql = " AND ".join(where_clauses)

    offset = (page - 1) * page_size

    # 🔹 COUNT QUERY
    count_query = text(f"""
        SELECT COUNT(DISTINCT hth.id)
        FROM ht_order_header hth
        LEFT JOIN tbl_warehouse tw ON tw.id = hth.warehouse_id
        LEFT JOIN tbl_areas ta ON ta.id = tw.area_id
        LEFT JOIN tbl_region tr ON tr.id = ta.region_id
        WHERE {where_sql}
    """)

    # 🔹 DATA QUERY
    data_query = text(f"""
        SELECT
            hth.id AS order_id,
            hth.order_code,
            hth.sap_id AS order_sap_id,
            hth.total,
            tw.warehouse_name,
            htdh.sap_id AS delivery_sap_id,
            htih.sap_id AS invoice_sap_id,
            COUNT(DISTINCT htdd.item_id) AS unique_item_count
        FROM ht_order_header hth
        LEFT JOIN tbl_warehouse tw ON tw.id = hth.warehouse_id
        LEFT JOIN tbl_areas ta ON ta.id = tw.area_id
        LEFT JOIN tbl_region tr ON tr.id = ta.region_id
        LEFT JOIN ht_delivery_header htdh ON htdh.order_id = hth.id
        LEFT JOIN ht_invoice_header htih ON htih.order_id = hth.id
        LEFT JOIN ht_order_detail htdd ON htdd.header_id = hth.id
        WHERE {where_sql}
        GROUP BY
            hth.id,
            hth.order_code,
            hth.sap_id,
            hth.total,
            tw.warehouse_name,
            htdh.sap_id,
            htih.sap_id
        ORDER BY hth.order_date DESC
        LIMIT :limit OFFSET :offset
    """)
    print(data_query)

    params["limit"] = page_size
    params["offset"] = offset

    try:
        with engine.connect() as conn:
            total_rows = conn.execute(count_query, params).scalar()
            rows = conn.execute(data_query, params).fetchall()

        total_pages = math.ceil(total_rows / page_size)

        base_url = str(request.url).split("?")[0]

        next_page = (
            f"{base_url}?page={page + 1}"
            if page < total_pages else None
        )
        previous_page = (
            f"{base_url}?page={page - 1}"
            if page > 1 else None
        )

        return {
            "total_rows": total_rows,
            "total_pages": total_pages,
            "current_page": page,
            "next_page": next_page,
            "previous_page": previous_page,
            "data": [dict(r._mapping) for r in rows]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


