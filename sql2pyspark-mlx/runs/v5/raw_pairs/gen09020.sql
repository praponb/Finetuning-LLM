select
    id,
    service,
    host,
    log_ts,
    status_code,
    payload_json,
    shiftright(status_code, 2) as status_code_high,
    coalesce(context['region'], 'none') as region_or_default,
    coalesce(context['version'], 'unknown') as version_or_default
from app_logs
where service = 'billing';
