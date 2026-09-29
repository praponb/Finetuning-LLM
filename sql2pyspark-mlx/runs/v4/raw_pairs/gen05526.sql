select
    id,
    log_ts,
    status_code,
    payload_json,
    context,
    size(context) as num_context,
    array_contains(map_keys(context), 'version') as has_version,
    coalesce(context['version'], 'none') as version_or_default
from app_logs
