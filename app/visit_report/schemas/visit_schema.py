from pydantic import BaseModel

class VisitSchema(BaseModel):
    from_date: str
    to_date: str
    warehouse_ids: list[int] = None
    route_ids: list[int] = None
    salesman_ids: list[int] = None