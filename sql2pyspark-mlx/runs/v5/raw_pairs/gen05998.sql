select
    id,
    service,
    log_ts,
    level,
    attr_key,
    attr_value
from app_logs
lateral view outer explode(context) x as attr_key, attr_value
