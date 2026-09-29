select
    id,
    service,
    host,
    log_ts,
    level,
    status_code,
    context,
    context['region'] as region,
    context['version'] as version,
    cast((status_code & 1) != 0 as int) as is_flagged,
    context['trace'] as trace
from app_logs
