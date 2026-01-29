from fastapi import FastAPI
from app.item_report.routes.item_filter import router as item_filters
from app.item_report.routes.item_dashboard import router as item_dashboard
from app.item_report.routes.item_table import router as item_table
from app.item_report.routes.item_export import router as item_export

from app.customer_report.routes.customer_filters import router as customer_filters
from app.customer_report.routes.customer_sales_dashboard import router as customer_dashboard
from app.customer_report.routes.customer_table import router as customer_table
from app.customer_report.routes.customer_sales_export import router as customer_export

from app.sales_report.routes.sales_filters import router as sales_filters  
from app.sales_report.routes.sales_dashboard import router as sales_dashboard
from app.sales_report.routes.sales_table import router as sales_table
from app.sales_report.routes.sales_export import router as sales_export

from app.attendance_report.routes.attendance_filter import router as attendance_filter
from app.attendance_report.routes.attendance_table import router as attendance_table

from app.primary_order_report.routes.pmry_ord_dashboard import router as pmry_ord_dashboard

from app.comparison_report.routes.comparison_filter import router as comparison_filter
from app.comparison_report.routes.comparison_table import router as comparison_table
from app.comparison_report.routes.comparison_dashboard import router as comparison_dashboard

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Hariss BI Backend (Customer Sales Reporting)")

app.include_router(item_filters, prefix="/api")
app.include_router(item_dashboard, prefix="/api")
app.include_router(item_table, prefix="/api")
app.include_router(item_export, prefix="/api")

app.include_router(customer_filters, prefix="/api")
app.include_router(customer_dashboard, prefix="/api")
app.include_router(customer_table, prefix="/api")
app.include_router(customer_export, prefix="/api")

app.include_router(sales_filters, prefix="/api")
app.include_router(sales_dashboard, prefix="/api")
app.include_router(sales_table, prefix="/api")
app.include_router(sales_export, prefix="/api")   

app.include_router(attendance_filter, prefix="/api")
app.include_router(attendance_table, prefix="/api")

app.include_router(pmry_ord_dashboard, prefix="/api")

app.include_router(comparison_filter, prefix="/api")
app.include_router(comparison_table, prefix="/api")
app.include_router(comparison_dashboard, prefix="/api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    )
