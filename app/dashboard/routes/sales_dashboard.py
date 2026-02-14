from fastapi import APIRouter
from sqlalchemy import text
from app.dashboard.schemas.dashboard_schema import DashboardRequest
from app.database import engine
from app.dashboard.utils.dashboard_common_helper import (
    validate_mandatory,
    sales_build_query_parts,
    purchase_build_query_parts,
    return_build_query_parts,
    choose_granularity,
    choose_return_granularity,
    choose_purchase_granularity,
    quantity_expr_sql,
    purchase_quantity_expr_sql,
    return_quantity_expr_sql,
)

router = APIRouter(tags=["Sales dashboard"])


@router.post("/sales-dash-region-sale")
def sales_dash_region_sale(filters: DashboardRequest):
    validate_mandatory(filters)

    granularity, period_label_sql, order_by_sql = choose_granularity(
        filters.from_date, filters.to_date
    )

    where_fragments, params = sales_build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    out = {"granularity": granularity, "charts": {}, "trend_line": {}}

    with engine.connect() as conn:
        quantity = quantity_expr_sql()
        sale_base_sql = f"""
            FROM invoice_headers ih
            JOIN invoice_details id ON id.header_id = ih.id
            JOIN tbl_warehouse w ON w.id = ih.warehouse_id
            JOIN tbl_region r ON r.id = w.region_id
            LEFT JOIN (
            SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
            FROM item_uoms
            GROUP BY item_id
            ) iu ON iu.item_id = id.item_id
            WHERE {where_sql}
            """
        # region wise sales
        query = f"""
            SELECT
            r.region_code || '-' || r.region_name AS region_name,
            {quantity} AS value
            {sale_base_sql}
            GROUP BY r.region_name, r.region_code
            ORDER BY value DESC
            """
        rows = conn.execute(text(query), params).fetchall()
        out["charts"]["region_performance"] = [dict(r._mapping) for r in rows]

        # region wise sales trend
        query = f"""
                SELECT 
                {period_label_sql} AS period_label,
                r.region_code || '-' || r.region_name AS region_name,
                {quantity} AS value
                {sale_base_sql}
                GROUP BY period_label,r.region_name,r.region_code,{order_by_sql}
                ORDER BY {order_by_sql}         
                """
        rows = conn.execute(text(query), params).fetchall()
        out["trend_line"]["sales_trend"] = [dict(r._mapping) for r in rows]

        return out


@router.post("/sales-dash-region-purchase")
def sales_dash_region_purchase(filters: DashboardRequest):
    validate_mandatory(filters)

    p_where_fragments, p_params = purchase_build_query_parts(filters)
    p_where_sql = " AND ".join(p_where_fragments)
   

    p_gran, p_period_sql, p_order_sql = choose_purchase_granularity(
        filters.from_date, filters.to_date
    )

    out = {"granularity": p_gran, "charts": {}, "trend_line": {}}

    with engine.connect() as conn:

        purchase_quantity = purchase_quantity_expr_sql()
        purchase_base_sql = f"""
                FROM ht_invoice_header hih
                JOIN ht_invoice_detail hid ON hid.header_id = hih.id
                JOIN tbl_warehouse w ON w.id = hih.warehouse_id
                JOIN tbl_region r ON r.id = w.region_id
                LEFT JOIN item_uoms iu ON iu.item_id = hid.item_id
                WHERE {p_where_sql}
                """
        # region wise purchase
        query = f"""
                SELECT
                r.region_code || '-' || r.region_name AS region_name,
                {purchase_quantity} AS value
                {purchase_base_sql}
                GROUP BY r.region_name, r.region_code
                ORDER BY value DESC
                """
        rows = conn.execute(text(query), p_params).fetchall()
        out["charts"]["purchase_region_performance"] = [dict(r._mapping) for r in rows]

        # region wise purchase trend
        query = f""" 
                        SELECT
                        {p_period_sql} AS period_label,
                        r.region_code || '-' || r.region_name AS region_name,
                        {purchase_quantity} AS purchase_quantity
                        {purchase_base_sql}
                        GROUP BY period_label,r.region_name,r.region_code,{p_order_sql}
                        ORDER BY {p_order_sql}
                        """
        rows = conn.execute(text(query), p_params).fetchall()
        out["trend_line"]["purchase_trend"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/sales-dash-region-return")
def sales_dash_region_return(filters: DashboardRequest):
    validate_mandatory(filters)
    r_where_fragments, r_params = return_build_query_parts(filters)
    r_where_sql = " AND ".join(r_where_fragments)
   

    r_gran, r_period_sql, r_order_sql = choose_return_granularity(
        filters.from_date, filters.to_date
    )

    out = {"granularity": r_gran, "charts": {}, "trend_line": {}}

    with engine.connect() as conn:

        return_quantity = return_quantity_expr_sql()
        return_base_sql = f"""
                        FROM ht_return_header hrh
                        JOIN ht_return_details hrd ON hrd.header_id = hrh.id
                        JOIN tbl_warehouse w ON w.id = hrh.warehouse_id
                         JOIN tbl_region r ON r.id = w.region_id
                        LEFT JOIN item_uoms iu ON iu.item_id = hrd.item_id
                        WHERE {r_where_sql}
                        """
        # region wise return
        query = f"""
                        SELECT
                        r.region_code || '-' || r.region_name AS region_name,
                        {return_quantity} AS value
                        {return_base_sql}
                        GROUP BY r.region_name,r.region_code
                        ORDER BY value DESC
                        """
        rows = conn.execute(text(query), r_params).fetchall()
        out["charts"]["return_region_performance"] = [dict(r._mapping) for r in rows]

        # region wise return trend
        query = f"""
                        SELECT
                        {r_period_sql} AS period_label,
                        r.region_code || '-' || r.region_name AS region_name,
                        {return_quantity} AS return_quantity
                        {return_base_sql}
                        GROUP BY period_label,r.region_name,r.region_code,{r_order_sql}
                        ORDER BY {r_order_sql}
                        """
        rows = conn.execute(text(query), r_params).fetchall()
        out["trend_line"]["return_trend"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/sales-dash-area-sale")
def sales_dash_area_sale(filters: DashboardRequest):
    validate_mandatory(filters)

    where_fragments, params = sales_build_query_parts(filters)
    where_sql = " AND ".join(where_fragments)

    granularity, period_label_sql, order_by_sql = choose_granularity(
        filters.from_date, filters.to_date
    )
    out = {"granularity": granularity, "charts": {}, "trend_line": {}}

    with engine.connect() as conn:
        quantity = quantity_expr_sql()
        sale_base_sql = f"""
                FROM invoice_headers ih
                JOIN invoice_details id ON id.header_id = ih.id
                JOIN tbl_warehouse w ON w.id = ih.warehouse_id
                JOIN tbl_areas a ON a.id = w.area_id
                LEFT JOIN (
                SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                FROM item_uoms
                GROUP BY item_id
                ) iu ON iu.item_id = id.item_id
                WHERE {where_sql}
                """
        # Area wise sales
        query = f"""
                SELECT 
                a.area_code || '-' || a.area_name AS area_name,
                {quantity} AS value
                {sale_base_sql}
                GROUP BY a.area_name, a.area_code
                ORDER BY value DESC
                """
        rows = conn.execute(text(query), params).fetchall()
        out["charts"]["area_performance"] = [dict(r._mapping) for r in rows]

        # Area wise sales trend
        # query = f"""
        #         SELECT
        #         {period_label_sql} AS period_label,
        #         a.area_code || '-' || a.area_name AS area_name,
        #         {quantity} AS value
        #         {sale_base_sql}
        #         GROUP BY period_label, a.area_name, a.area_code, {order_by_sql}
        #         ORDER BY {order_by_sql}
        #         """
        # rows = conn.execute(text(query), params).fetchall()
        # out["trend_line"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/sales-dash-area-purchase")
def sales_dash_area_purchase(filters: DashboardRequest):
    validate_mandatory(filters)

    p_where_fragments, p_params = purchase_build_query_parts(filters)
    p_where_sql = " AND ".join(p_where_fragments)

    p_gran, p_period_sql, p_order_sql = choose_purchase_granularity(
        filters.from_date, filters.to_date
    )

    out = {"granularity": p_gran, "charts": {}, "trend_line": {}}

    with engine.connect() as conn:

        purchase_quantity = purchase_quantity_expr_sql()
        purchase_base_sql = f"""
                FROM ht_invoice_header hih
                JOIN ht_invoice_detail hid ON hid.header_id = hih.id
                JOIN tbl_warehouse w ON w.id = hih.warehouse_id
                JOIN tbl_areas a ON a.id = w.area_id
                LEFT JOIN item_uoms iu ON iu.item_id = hid.item_id
                WHERE {p_where_sql}
                """

        # Area wise purchase
        query = f"""
                SELECT
                a.area_code || '-' || a.area_name AS area_name,
                {purchase_quantity} AS value
                {purchase_base_sql}
                GROUP BY a.area_name, a.area_code
                ORDER BY value DESC
                """
        rows = conn.execute(text(query), p_params).fetchall()
        out["charts"]["purchase_area_performance"] = [dict(r._mapping) for r in rows]

        # Area wise purchase trend
        # query = f"""
        #                 SELECT
        #                 {p_period_sql} AS period_label,
        #                 a.area_code || '-' || a.area_name AS area_name,
        #                 {purchase_quantity} AS value
        #                 {purchase_base_sql}
        #                 GROUP BY period_label,a.area_name,a.area_code,{p_order_sql}
        #                 ORDER BY {p_order_sql}  
        #                 """
        # rows = conn.execute(text(query), p_params).fetchall()
        # out["trend_line"] = [dict(r._mapping) for r in rows]

    return out


@router.post("/sales-dash-area-return")
def sales_dash_area_return(filters: DashboardRequest):
    validate_mandatory(filters)

    r_where_fragments, r_params = return_build_query_parts(filters)
    r_where_sql = " AND ".join(r_where_fragments)
    
    r_gran, r_period_sql, r_order_sql = choose_return_granularity(
        filters.from_date, filters.to_date
    )

    out = {"granularity": r_gran, "charts": {}, "trend_line": {}}

    with engine.connect() as conn:

        return_quantity = return_quantity_expr_sql()
        return_base_sql = f"""
                        FROM ht_return_header hrh
                        JOIN ht_return_details hrd ON hrd.header_id = hrh.id
                        JOIN tbl_warehouse w ON w.id = hrh.warehouse_id
                        JOIN tbl_areas a ON a.id = w.area_id
                        LEFT JOIN item_uoms iu ON iu.item_id = hrd.item_id
                        WHERE {r_where_sql}
                        """

        # Area wise return
        query = f"""
                        SELECT
                        a.area_code || '-' || a.area_name AS area_name,
                        {return_quantity} AS value
                        {return_base_sql}
                        GROUP BY a.area_name, a.area_code
                        ORDER BY value DESC
                        """
        rows = conn.execute(text(query), r_params).fetchall()
        out["charts"]["return_area_performance"] = [dict(r._mapping) for r in rows]

        # Area wise return trend
        # query = f"""
        #                 SELECT
        #                 {r_period_sql} AS period_label,
        #                 a.area_code || '-' || a.area_name AS area_name,
        #                 {return_quantity} AS value
        #                 {return_base_sql}
        #                 GROUP BY period_label,a.area_name,a.area_code,{r_order_sql}
        #                 ORDER BY {r_order_sql}
        #                 """
        # rows = conn.execute(text(query), r_params).fetchall()
        # out["trend_line"]= [dict(r._mapping) for r in rows]

    return out


@router.post("/sales-dash-warehouse-sale")
def sales_dash_warehouse_sale(filters:DashboardRequest):
        validate_mandatory(filters)

        where_fragments, params = sales_build_query_parts(filters)
        where_sql = " AND ".join(where_fragments)
        

        granularity, period_label_sql, order_by_sql = choose_granularity(
        filters.from_date, filters.to_date
        )

        out = {"granularity": granularity, "charts": {}, "trend_line": {}}

        with engine.connect() as conn:
                
                quantity = quantity_expr_sql()
                sale_base_sql = f"""
                        FROM invoice_headers ih
                        JOIN invoice_details id ON id.header_id = ih.id
                        JOIN tbl_warehouse w ON w.id = ih.warehouse_id
                        LEFT JOIN (
                        SELECT item_id, MAX(NULLIF(upc::numeric, 0)) AS upc
                        FROM item_uoms
                        GROUP BY item_id
                        ) iu ON iu.item_id = id.item_id
                        WHERE {where_sql}
                        """
                # Warehouse wise sales
                query = f"""
                        SELECT
                        w.warehouse_code || '-' || w.warehouse_name AS warehouse_name,
                        {quantity} AS value
                        {sale_base_sql}
                        GROUP BY w.warehouse_name, w.warehouse_code
                        ORDER BY value DESC
                        """
                rows = conn.execute(text(query), params).fetchall()
                out["charts"]["sales_warehouse_performance"] = [dict(r._mapping) for r in rows]

                # Warehouse wise sales trend
                # query = f"""
                #         SELECT
                #         {period_label_sql} AS period,
                #         w.warehouse_code || '-' || w.warehouse_name AS warehouse_name,
                #         {quantity} AS value
                #         {sale_base_sql}
                #         GROUP BY period, w.warehouse_name, w.warehouse_code,{order_by_sql}
                #         ORDER BY {order_by_sql}
                # """
                # rows = conn.execute(text(query), params).fetchall()
                # out["trend_line"] = [dict(r._mapping) for r in rows]

        return out

@router.post("/sales-dash-warehouse-purchase")
def sales_dash_warehouse_purchase(filters:DashboardRequest):
        validate_mandatory(filters)

        p_where_fragments, p_params = purchase_build_query_parts(filters)
        p_where_sql = " AND ".join(p_where_fragments)
        

        p_gran, p_period_sql, p_order_sql = choose_purchase_granularity(
        filters.from_date, filters.to_date
        )

        out = {"granularity":p_gran, "charts": {}, "trend_line": {}}

        with engine.connect() as conn:
             
                purchase_quantity = purchase_quantity_expr_sql()
                purchase_base_sql = f"""
                        FROM ht_invoice_header hih
                        JOIN ht_invoice_detail hid ON hid.header_id = hih.id
                        JOIN tbl_warehouse w ON w.id = hih.warehouse_id
                        LEFT JOIN item_uoms iu ON iu.item_id = hid.item_id
                        WHERE {p_where_sql}
                        """
                
                query = f"""
                        SELECT
                        w.warehouse_code || '-' || w.warehouse_name AS warehouse_name,
                        {purchase_quantity} AS value
                        {purchase_base_sql}
                        GROUP BY w.warehouse_name, w.warehouse_code
                        ORDER BY value DESC
                        """
                rows = conn.execute(text(query), p_params).fetchall()
                out["charts"]["purchase_warehouse_performance"] = [dict(r._mapping) for r in rows]
                
                # Warehouse wise purchase trend
                # query = f"""
                #         SELECT
                #         {p_period_sql} AS period,
                #         w.warehouse_code || '-' || w.warehouse_name AS warehouse_name,
                #         {purchase_quantity} AS value
                #         {purchase_base_sql}
                #         GROUP BY period, w.warehouse_name, w.warehouse_code,{p_order_sql}
                #         ORDER BY {p_order_sql}
                # """
                # rows = conn.execute(text(query), p_params).fetchall()
                # out["trend_line"] = [dict(r._mapping) for r in rows]

        return out
                
@router.post("/sales-dash-warehouse-return")
def sales_dash_warehouse_return(filters:DashboardRequest):
        validate_mandatory(filters)

        r_where_fragments, r_params = return_build_query_parts(filters)
        r_where_sql = " AND ".join(r_where_fragments)
        

        r_gran, r_period_sql, r_order_sql = choose_return_granularity(
        filters.from_date, filters.to_date
        )

        out = {"granularity":r_gran, "charts": {}, "trend_line": {}}

        with engine.connect() as conn:
             
                return_quantity = return_quantity_expr_sql()
                return_base_sql = f"""
                        FROM ht_return_header hrh
                        JOIN ht_return_details hrd ON hrd.header_id = hrh.id
                        JOIN tbl_warehouse w ON w.id = hrh.warehouse_id
                        LEFT JOIN item_uoms iu ON iu.item_id = hrd.item_id
                        JOIN tbl_areas a ON a.id = w.area_id
                        WHERE {r_where_sql}
                        """
                
                # Warehouse wise return
                query = f"""
                        SELECT
                        w.warehouse_code || '-' || w.warehouse_name AS warehouse_name,
                        {return_quantity} AS value
                        {return_base_sql}
                        GROUP BY w.warehouse_name, w.warehouse_code
                        ORDER BY value DESC
                        """
                rows = conn.execute(text(query), r_params).fetchall()
                out["charts"]["return_warehouse_performance"] = [dict(r._mapping) for r in rows]

                # Warehouse wise return trend
                # query = f"""
                #         SELECT
                #         {r_period_sql} AS period_label,
                #         w.warehouse_code || '-' || w.warehouse_name AS warehouse_name,
                #         {return_quantity} AS value
                #         {return_base_sql}
                #         GROUP BY period_label,w.warehouse_name,w.warehouse_code,{r_order_sql}
                #         ORDER BY {r_order_sql}
                # """
                # rows = conn.execute(text(query), r_params).fetchall()
                # out["trend_line"]= [dict(r._mapping) for r in rows]

        return out
                