with flattened_attrs as (
    -- Higher-order array functions
    select
        id,
        service,
        latency_ms,
        status_code,
        payload_json,
        context,
        get_json_object(payload_json, '$.duration') as duration_text,
        array_contains(map_keys(context), 'version') as has_version,
        size(context) as num_context,
        context['trace'] as trace
    from app_logs
)
-- Ranking and gap features
select
    *,
    lead(num_context) over (partition by service order by status_code, id) next_num_context,
    lag(status_code) over (partition by service order by status_code, id) prev_status_code,
    row_number() over (partition by service order by status_code, id) event_seq
from flattened_attrs
