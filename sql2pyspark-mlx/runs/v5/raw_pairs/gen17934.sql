select
    id,
    service,
    latency_ms,
    status_code,
    key,
    v
from app_logs
lateral view outer explode(context) e as key, v
