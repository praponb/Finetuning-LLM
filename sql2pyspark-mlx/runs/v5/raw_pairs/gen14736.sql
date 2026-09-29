with unpacked as (
    select
        id,
        log_ts,
        latency_ms,
        context,
        array_contains(map_keys(context), 'trace') as has_trace,
        context['version'] as version,
        coalesce(context['version'], 'direct') as version_or_default,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        shiftright(status_code, 2) as status_code_high
    from app_logs
    where latency_ms >= 1081
)
select
    *,
    sum(latency_ms) over (order by latency_ms, id rows between unbounded preceding and current row) running_sum_latency_ms,
    avg(latency_ms) over (order by latency_ms, id rows between unbounded preceding and current row) running_avg_latency_ms,
    min(latency_ms) over (order by latency_ms, id rows between unbounded preceding and current row) running_min_latency_ms,
    percent_rank() over (order by latency_ms desc nulls last) latency_ms_percent_rank
from unpacked
