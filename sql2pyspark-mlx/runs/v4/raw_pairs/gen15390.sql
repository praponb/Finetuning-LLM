select
    id,
    service,
    latency_ms,
    k,
    value
from app_logs
lateral view outer explode(context) e as k, value
