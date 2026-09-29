select
    id,
    service,
    host,
    level,
    latency_ms,
    status_code,
    attr_key,
    attr_value
from app_logs
lateral view outer explode(context) e as attr_key, attr_value
