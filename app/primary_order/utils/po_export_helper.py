from sqlalchemy import text

def build_order_summary_query(where_sql: str):
    return text(f"""
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

            LEFT JOIN tbl_warehouse tw
                ON tw.id = hth.warehouse_id

            LEFT JOIN tbl_areas ta
                ON ta.id = tw.area_id

            LEFT JOIN tbl_region tr
                ON tr.id = ta.region_id

            LEFT JOIN ht_delivery_header htdh
                ON htdh.order_id = hth.id

            LEFT JOIN ht_invoice_header htih
                ON htih.order_id = hth.id

            LEFT JOIN ht_order_detail htdd
                ON htdd.header_id = hth.id


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
    """)


def build_filters(filters):
    where_clauses = []
    params = {}

    # Date (mandatory)
    where_clauses.append(
        "hth.order_date BETWEEN :from_date AND :to_date"
    )
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    # Company
    if filters.company_ids:
        where_clauses.append("hth.company_id = ANY(:company_ids)")
        params["company_ids"] = filters.company_ids

    # Region (via tbl_region)
    if filters.region_ids:
        where_clauses.append("tr.id = ANY(:region_ids)")
        params["region_ids"] = filters.region_ids

    # Area (via tbl_areas)
    if filters.area_ids:
        where_clauses.append("ta.id = ANY(:area_ids)")
        params["area_ids"] = filters.area_ids

    # Warehouse
    if filters.warehouse_ids:
        where_clauses.append("tw.id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = filters.warehouse_ids

    return " AND ".join(where_clauses), params

