select
    id,
    service,
    level,
    status_code,
    k,
    value
from app_logs
lateral view outer explode(context) x as k, value;
