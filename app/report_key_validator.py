from fastapi import HTTPException
from sqlalchemy import text
import re

def get_user_from_report_key(report_key: str, conn):

    if not report_key:
        raise HTTPException(status_code=401, detail="Report key required")

    report_key = report_key.strip()

    # Validate format: reportkey_<user_id>_<random>
    match = re.match(r"^reportkey_(\d+)_", report_key)

    if not match:
        raise HTTPException(status_code=401, detail="Invalid report key format")

    extracted_user_id = int(match.group(1))

    row = conn.execute(text("""
        SELECT user_id
        FROM report_keys
        WHERE api_key = :key
        AND user_id = :uid
        AND is_active = true
    """), {
        "key": report_key,
        "uid": extracted_user_id
    }).fetchone()

    if not row:
        raise HTTPException(status_code=401, detail="Invalid or inactive report key")

    return extracted_user_id