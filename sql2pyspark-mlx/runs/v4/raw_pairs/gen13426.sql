select
    id,
    host,
    log_ts,
    level,
    latency_ms,
    status_code,
    k,
    v
from app_logs
lateral view explode(context) t as k, v
