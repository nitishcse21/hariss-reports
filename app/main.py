from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.sales_report.routes.sales_filters import router as sales_filters  
from app.sales_report.routes.sales_dashboard import router as sales_dashboard
from app.sales_report.routes.sales_table import router as sales_table
from app.sales_report.routes.sales_export import router as sales_export

from app.customer_report.routes.customer_filters import router as customer_filters
from app.customer_report.routes.customer_sales_dashboard import router as customer_dashboard
from app.customer_report.routes.customer_table import router as customer_table
from app.customer_report.routes.customer_sales_export import router as customer_export

from app.item_report.routes.item_filter import router as item_filters
from app.item_report.routes.item_dashboard import router as item_dashboard
from app.item_report.routes.item_table import router as item_table
from app.item_report.routes.item_export import router as item_export


from app.attendance_report.routes.attendance_filter import router as attendance_filter
from app.attendance_report.routes.attendance_table import router as attendance_table
from app.attendance_report.routes.attendance_export import router as attendence_export

from app.primary_order.routes.po_filtr import router as po_order_filter
from app.primary_order.routes.po_table import router as po_order_table
from app.primary_order.routes.po_order_export import router as po_order_export
from app.primary_order.routes.pmry_ord_dashboard import router as po_order_dashboard

from app.comparison_report.routes.comparison_export import router as comparison_export
from app.comparison_report.routes.comparison_filter import router as comparison_filter
from app.comparison_report.routes.comparison_table import router as comparison_table
from app.comparison_report.routes.comparison_dashboard import router as comparison_dashboard

from app.load_unload_report.routes.load_unload_filter import router as load_unload_filter
from app.load_unload_report.routes.load_unload_table import router as load_unload_table
from app.load_unload_report.routes.load_unload_export import router as load_unload_export
from app.load_unload_report.routes.load_unload_dashboard import router as load_unload_dashboard

from app.visit_report.routes.visit_dashboard import router as visit_dashboard
from app.visit_report.routes.visit_table import router as visit_table
from app.visit_report.routes.visit_export import router as visit_export
from app.visit_report.routes.visit_filter import router as visit_filter

app = FastAPI(title="Hariss BI Backend Reporting")



app.include_router(sales_filters, prefix="/api")
app.include_router(sales_dashboard, prefix="/api")
app.include_router(sales_table, prefix="/api")
app.include_router(sales_export, prefix="/api") 

app.include_router(customer_filters, prefix="/api")
app.include_router(customer_dashboard, prefix="/api")
app.include_router(customer_table, prefix="/api")
app.include_router(customer_export, prefix="/api")


app.include_router(item_filters, prefix="/api")
app.include_router(item_dashboard, prefix="/api")
app.include_router(item_table, prefix="/api")
app.include_router(item_export, prefix="/api")


app.include_router(attendance_filter, prefix="/api")
app.include_router(attendance_table, prefix="/api")
app.include_router(attendence_export, prefix="/api")

app.include_router(po_order_filter, prefix="/api")
app.include_router(po_order_table, prefix="/api")
app.include_router(po_order_export, prefix="/api")
app.include_router(po_order_dashboard, prefix="/api")

app.include_router(comparison_filter, prefix="/api")
app.include_router(comparison_export, prefix="/api")
app.include_router(comparison_table, prefix="/api")
app.include_router(comparison_dashboard, prefix="/api")

app.include_router(load_unload_filter, prefix="/api")
app.include_router(load_unload_table, prefix="/api")
app.include_router(load_unload_export, prefix="/api")
app.include_router(load_unload_dashboard, prefix="/api")

app.include_router(visit_filter, prefix="/api")
app.include_router(visit_dashboard, prefix="/api")
app.include_router(visit_export, prefix="/api")
app.include_router(visit_table, prefix="/api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    )
