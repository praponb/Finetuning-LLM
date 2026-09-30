select
    id,
    service,
    host,
    key,
    v
from app_logs
lateral view outer explode(context) t as key, v;
