select
    id,
    service,
    host,
    level,
    latency_ms,
    status_code,
    payload_json,
    context,
    get_json_object(payload_json, '$.duration') as duration_text,
    cast((status_code & 4) != 0 as int) as has_fault,
    coalesce(context['trace'], 'unknown') as trace_or_default
from app_logs
