select
    id,
    service,
    k,
    value
from app_logs
lateral view outer explode(context) e as k, value;
