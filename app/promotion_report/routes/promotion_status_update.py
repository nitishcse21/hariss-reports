from fastapi import APIRouter, HTTPException, Query, Request, Depends
from app.promotion_report.schemas.promotion_schema import PromotionActionRequest
from app.database import engine
from sqlalchemy import text
from datetime import datetime
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.report_key_validator import get_user_from_report_key

router = APIRouter()
security = HTTPBearer()


@router.post("/promotion-action")
def promotion_action(payload: PromotionActionRequest, request: Request,credentials: HTTPAuthorizationCredentials = Depends(security)):
    report_key = credentials.credentials.strip()

    with engine.begin() as conn:
        
        user_id = get_user_from_report_key(report_key, conn)
        invoice_ids = payload.invoice_ids
        action = payload.action.lower()
        user_id = user_id 
        comment = payload.comment

   

        role_id = conn.execute(text("""
            SELECT role
            FROM users
            WHERE id = :user_id
        """), {"user_id": user_id}).scalar()

        row = conn.execute(
            text(
                """
            SELECT approver_id, rm_approver_id, rejected_by, rm_reject_id
            FROM invoice_details
            WHERE id = Any(:invoice_ids)
            """
            ),
            {"invoice_ids": invoice_ids},
        ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Invoice not found")

        if role_id == 91:  # ASM
            if action == "approve":
                msg = "Promotion approved successfully"
                conn.execute(
                    text(
                        """
                    UPDATE invoice_details
                    SET approver_id = :user_id,
                        approved_date = :now
                    WHERE id = Any(:invoice_ids)
                    """
                    ),
                    {
                        "user_id": user_id,
                        "invoice_ids": invoice_ids,
                        "now": datetime.now(),
                    },
                )

            elif action == "reject":
                msg = "Promotion rejected successfully"
                conn.execute(
                    text(
                        """
                    UPDATE invoice_details
                    SET rejected_by = :user_id,
                        comment_for_rejection = :comment
                    WHERE id = Any(:invoice_ids)
                    """
                    ),
                    {"user_id": user_id, "invoice_ids": invoice_ids, "comment": comment},
                )

            else:
                raise HTTPException(status_code=400, detail="Invalid ASM action")

        elif role_id == 92:  # RSM
            if action == "approve":
                msg = "Promotion approved successfully"
                conn.execute(
                    text(
                        """
                    UPDATE invoice_details
                    SET rm_approver_id = :user_id,
                        rmaction_date = :now
                    WHERE id = Any(:invoice_ids)
                    """
                    ),
                    {
                        "user_id": user_id,
                        "invoice_ids": invoice_ids,
                        "now": datetime.now(),
                    },
                )

            elif action == "reject":
                msg = "Promotion rejected successfully"
                conn.execute(
                    text(
                        """
                    UPDATE invoice_details
                    SET rm_reject_id = :user_id,
                        comment_for_rejection = :comment
                    WHERE id = Any(:invoice_ids)
                    """
                    ),
                    {"user_id": user_id, "invoice_ids": invoice_ids, "comment": comment},
                )
            else:
                raise HTTPException(status_code=400, detail="Invalid RSM action")
        else:
            raise HTTPException(status_code=400, detail="Invalid role")

    return {
        "message": msg,
        "invoice_ids": invoice_ids,
        "action": action,
        "role_id": role_id,
    }
