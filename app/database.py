from urllib.parse import quote_plus
from sqlalchemy import create_engine

raw_password = ">V4H?Q!6PZwXw$+C"

encoded_password = quote_plus(raw_password)




engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
