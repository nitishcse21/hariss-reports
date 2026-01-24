from typing import Optional, List,Tuple,Dict
from app.attendance_report.schemas.attendance_schema import AttendanceRequest
from fastapi import HTTPException
from datetime import datetime


def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None
    

def validate_mandatory(filters:AttendanceRequest):
    if not filters.from_date or not filters.to_date or not filters.search_type:
        raise HTTPException(status_code=400, detail="from_date, to_date, and search_type are required")
    try:
        _ = datetime.fromisoformat(filters.from_date)
        _ = datetime.fromisoformat(filters.to_date)
    except Exception:
        raise HTTPException(status_code=400, detail="from_date/to_date must be in YYYY-MM-DD format")
    


def build_query_parts(filters: AttendanceRequest):

    joins = []
    where_fragments = []
    params = {}

    where_fragments.append(
        "sa.attendance_date BETWEEN :from_date AND :to_date"
    )
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    
    if filters.warehouse_ids:
        where_fragments.append(
        "string_to_array(s.warehouse_id, ',') && string_to_array(:warehouse_ids, ',')"
        )
        params["warehouse_ids"] = ",".join(map(str, filters.warehouse_ids))


    if filters.salesman_ids:
        where_fragments.append(
            "s.id = ANY(:salesman_ids)"
        )
        params["salesman_ids"] = filters.salesman_ids

    joins = list(dict.fromkeys(joins))
    return joins, where_fragments, params
