from app.customer_report.schemas.customer_sales_schema import DownloadRequest
# -------------------------------------------------------------------
# WHERE CLAUSE BUILDER
# -------------------------------------------------------------------

def build_where(payload: DownloadRequest):
    where = ["invoice_date BETWEEN :from_date AND :to_date"]
    params = {
        "from_date": payload.from_date,
        "to_date": payload.to_date,
    }

    if payload.company_ids:
        where.append("company_id = ANY(:company_ids)")
        params["company_ids"] = payload.company_ids

    if payload.region_ids:
        where.append("region_id = ANY(:region_ids)")
        params["region_ids"] = payload.region_ids

    if payload.area_ids:
        where.append("area_id = ANY(:area_ids)")
        params["area_ids"] = payload.area_ids

    if payload.warehouse_ids:
        where.append("warehouse_id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = payload.warehouse_ids

    if payload.route_ids:
        where.append("route_id = ANY(:route_ids)")
        params["route_ids"] = payload.route_ids

    if payload.display_quantity == "without_free_good":
        where.append("total_amount > 0")

    return " AND ".join(where), params

# -------------------------------------------------------------------
# SQL TEMPLATES
# -------------------------------------------------------------------

DEFAULT_SQL = """
SELECT
    ms.customer_code || ' - ' || ms.customer_name AS "Customer Name",
    ms.customer_channel AS "Customer Channel Name",
    ms.customer_category_name AS "Customer Category Name",
    ms.contact_no AS "Contact Number",
    ms.warehouse_name AS "Warehouse Name",
    ms.route_name AS "Route Name",
    ROUND(COALESCE(SUM({value_expr}),0)::numeric,3) AS "{value_label}"
FROM mv_sales_report_fast ms
LEFT JOIN (
    SELECT
        item_id,
        MAX(upc::numeric) AS upc
    FROM item_uoms
    WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
    GROUP BY item_id
) iu ON iu.item_id = ms.item_id
WHERE {where_sql}
GROUP BY
    ms.customer_code, ms.customer_name,
    ms.customer_channel,
    ms.customer_category_name,
    ms.contact_no,
    ms.warehouse_name,
    ms.route_name
ORDER BY "{value_label}" DESC
"""
DETAIL_SQL = """
SELECT
    ms.customer_code || ' - ' || ms.customer_name AS "Customer Name",
    ms.customer_channel AS "Customer Channel Name",
    ms.customer_category_name AS "Customer Category Name",
    ms.contact_no AS "Contact Number",
    ms.warehouse_name AS "Warehouse Name",
    ms.route_name AS "Route Name",
    ms.item_code AS "Item Code",
    ms.item_name AS "Item Name",
    ms.item_category_name AS "Item Category Name",
    ROUND(COALESCE(SUM({value_expr}),0)::numeric,3) AS "{value_label}"
FROM mv_sales_report_fast ms
LEFT JOIN (
    SELECT
        item_id,
        MAX(upc::numeric) AS upc
    FROM item_uoms
    WHERE upc ~ '^[0-9]+(\\.[0-9]+)?$'
    GROUP BY item_id
) iu ON iu.item_id = ms.item_id
WHERE {where_sql}
GROUP BY
    ms.customer_code, ms.customer_name,
    ms.customer_channel,
    ms.customer_category_name,
    ms.contact_no,
    ms.warehouse_name,
    ms.route_name,
    ms.item_code,
    ms.item_name,
    ms.item_category_name
ORDER BY
    "Customer Name",
    "{value_label}" DESC
"""
