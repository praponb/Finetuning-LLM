with parsed as (
    -- Semi-structured parsing
    select
        id,
        service,
        log_ts,
        latency_ms,
        payload_json,
        size(context) as num_context,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec
    from app_logs
    where latency_ms = 1099
)
-- Aggregate with conditional metrics
select
    date_trunc('hour', log_ts) as hour_window,
    case when num_context >= 82 then 'LARGE' else 'SMALL' end as num_context_tier,
    count(distinct id) as distinct_ids,
    sort_array(collect_set(service)) as service_list,
    min(log_ts) as started_at
from parsed
group by grouping sets ((date_trunc('hour', log_ts), case when num_context >= 82 then 'LARGE' else 'SMALL' end), (date_trunc('hour', log_ts)))
order by num_context_tier desc nulls first;
