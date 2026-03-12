from fastapi import HTTPException
from typing import Optional,List,Tuple,Dict
from app.promotion_report.schemas.promotion_schema import PromotionRequest
from datetime import datetime



def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None
    
def validate_mandatory(filters:PromotionRequest):
    if not filters.from_date or not filters.to_date:
        raise HTTPException(status_code=400, detail="from_date and to_date are required")
    try:
        _ = datetime.fromisoformat(filters.from_date)
        _ = datetime.fromisoformat(filters.to_date)
    except Exception:
        raise HTTPException(status_code=400, detail="from_date/to_date must be in YYYY-MM-DD format")
    
def build_query_parts(
    filters: PromotionRequest,
) -> Tuple[List[str], List[str], Dict]:


    joins: List[str] = []
    where_fragments: List[str] = []
    params: Dict = {}

    where_fragments.append(
        "ih.invoice_date BETWEEN :from_date AND :to_date"
    )
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    where_fragments.append("idl.item_total = 0")

    if filters.company_ids:
        joins.append(
            "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
        )
        where_fragments.append(
            "ih.company_id = ANY(:company_ids)"
        )
        params["company_ids"] = filters.company_ids

    need_warehouse_join = False

    if filters.region_ids:
        joins.append(
            "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
        )
        where_fragments.append(
            "w.region_id = ANY(:region_ids)"
        )
        params["region_ids"] = filters.region_ids
        need_warehouse_join = True

    if filters.area_ids:
        if not need_warehouse_join:
            joins.append(
                "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
            )
            need_warehouse_join = True

        where_fragments.append(
            "w.area_id = ANY(:area_ids)"
        )
        params["area_ids"] = filters.area_ids

    if filters.warehouse_ids:
        where_fragments.append(
            "ih.warehouse_id = ANY(:warehouse_ids)"
        )
        params["warehouse_ids"] = filters.warehouse_ids
    
    status_case = """
                CASE
                    WHEN idl.rm_approver_id > 0
                        AND idl.rmaction_date IS NOT NULL
                    THEN 'approved by rsm'

                    WHEN idl.rm_reject_id > 0
                    THEN 'rejected by rsm'

                    WHEN idl.rejected_by > 0
                    THEN 'rejected by asm'

                    WHEN idl.approver_id > 0
                        AND idl.approved_date IS NOT NULL
                    THEN 'approved by asm'

                    ELSE 'pending'
                END
                """

    if filters.status:
        where_fragments.append(f"LOWER({status_case}) = LOWER(:status)")
        params["status"] = filters.status
        # if filters.status:

    #     if filters.status == "pending":
    #         where_fragments.append("""
    #             idl.approver_id IS NULL
    #             AND idl.rejected_by IS NULL
    #             AND idl.rm_approver_id IS NULL
    #             AND idl.rm_reject_id IS NULL
    #         """)

    #     elif filters.status == "approved by ASM":
    #         where_fragments.append("""
    #             idl.approver_id IS NOT NULL
    #             AND idl.approved_date IS NOT NULL
    #             AND idl.rm_approver_id IS NULL
    #             AND idl.rm_reject_id IS NULL
    #         """)

    #     elif filters.status == "rejected by ASM":
    #         where_fragments.append("""
    #             idl.rejected_by IS NOT NULL
    #             AND idl.comment_for_rejection IS NOT NULL
    #         """)

    #     elif filters.status == "rejected by RSM":
    #         where_fragments.append("""
    #             idl.rm_reject_id IS NOT NULL
    #             AND idl.comment_for_rejection IS NOT NULL
    #         """)

    #     elif filters.status == "approved by RSM":
    #         where_fragments.append("""
    #             idl.rm_approver_id IS NOT NULL
    #             AND idl.rmaction_date IS NOT NULL
    #         """)



    joins = list(dict.fromkeys(joins))

    return joins, where_fragments, params,status_case


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
