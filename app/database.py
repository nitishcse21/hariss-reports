from urllib.parse import quote_plus
from sqlalchemy import create_engine

raw_password = ">V4H?Q!6PZwXw$+C"

encoded_password = quote_plus(raw_password)

DB_URL = (
    f"postgresql://laravel_user:{encoded_password}"
    "@161.35.143.76:5432/productionDev"
)


engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
