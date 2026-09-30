select
    id,
    host,
    level,
    status_code,
    k,
    attr_value
from app_logs
lateral view explode(context) li as k, attr_value;
