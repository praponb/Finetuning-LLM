with parsed as (
    select
        id,
        service,
        log_ts,
        latency_ms,
        context,
        get_json_object(payload_json, '$.duration') as duration_text,
        array_contains(map_keys(context), 'version') as has_version,
        coalesce(context['trace'], 'none') as trace_or_default,
        context['region'] as region,
        get_json_object(payload_json, '$.user.id') as user_id
    from app_logs
)
select
    case when latency_ms >= 2154 then 'HIGH' else 'LOW' end as latency_ms_tier,
    date_trunc('hour', log_ts) as hour_window,
    (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as duration_min,
    approx_percentile(latency_ms, array(0.25, 0.5, 0.75)) as latency_ms_quartiles,
    count(distinct service) as distinct_services,
    grouping(case when latency_ms >= 2154 then 'HIGH' else 'LOW' end) as grouping_id
from parsed
group by rollup (case when latency_ms >= 2154 then 'HIGH' else 'LOW' end, date_trunc('hour', log_ts))
having count(distinct service) < 1000
