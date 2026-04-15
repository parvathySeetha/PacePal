import math
from decimal import Decimal
from core.helper import SalesforceClient

sf_client = SalesforceClient("demo")

def _to_float(value, default=0.0):
    try:
        if value is None:
            return default
        f_val = float(value)
        if math.isnan(f_val):
            return default
        return f_val
    except Exception:
        return default


def _to_decimal(value, default="0"):
    try:
        if value is None:
            return Decimal(str(default))
        return Decimal(str(value))
    except Exception:
        return Decimal(str(default))


def _safe_in_clause(ids):
    safe_ids = [f"'{str(i)}'" for i in ids if i]
    return ",".join(safe_ids)
