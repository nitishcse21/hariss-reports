from fastapi import APIRouter, Query
from schemas.attendance_schema import AttendanceRequest
from typing import Optional
from utils.common_helper import parse_csv_ids




router = APIRouter(prefix="/api")



@router.get("/attendance-filter")
def attendance_filter(
warehouse_ids: Optional[str] = Query(None),
salesman_ids:Optional[str]= Query(None)
):
    
