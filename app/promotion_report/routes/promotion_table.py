from fastapi import APIRouter
from app.promotion_report.schemas.promotion_schema import PromotionRequest
from app.promotion_report.utils.promotion_common_helper import validate_mandatory

router = APIRouter()

@router.post("/promotion-table")
def promotion_table(filters:PromotionRequest):
    validate_mandatory(filters)
    
    


