from fastapi import FastAPI
from item_report.routes.item_filter import app as item_filters_app
from item_report.routes.item_dashboard import app as item_dashboard
from item_report.routes.item_table import app as item_table_app
from item_report.routes.item_export import app as export_app
from customer_report.routes.customer_sales_dashboard import app as customer_sales_dashboard_app
from customer_report.routes.customer_table import app as customer_table_app
from customer_report.routes.customer_sales_download import app as customer_sales_download_app
from customer_report.routes.customer_filters import app as customer_filters_app
from sales_report.routes.dashboard import app as customer_sales_dashboard
from sales_report.routes.export_xlsx import app as customer_sales_export
from sales_report.routes.table import app as customer_sales_table
from sales_report.routes.filters import app as customer_sales_filters    
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Hariss BI Backend (Customer Sales Reporting)")

app.include_router(item_filters_app, prefix="/api")
app.include_router(item_dashboard, prefix="/api")
app.include_router(item_table_app, prefix="/api")
app.include_router(export_app, prefix="/api")
app.include_router(customer_sales_dashboard_app, prefix="/api")
app.include_router(customer_table_app, prefix="/api")
app.include_router(customer_sales_download_app, prefix="/api")
app.include_router(customer_filters_app, prefix="/api")
app.include_router(customer_sales_dashboard, prefix="/api")
app.include_router(customer_sales_export, prefix="/api")
app.include_router(customer_sales_table, prefix="/api")
app.include_router(customer_sales_filters, prefix="/api")   


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    )
