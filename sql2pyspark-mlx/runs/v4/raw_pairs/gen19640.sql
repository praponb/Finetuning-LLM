with flattened_attrs as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        log_ts,
        context['version'] as version,
        coalesce(context['region'], 'none') as region_or_default,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        coalesce(context['version'], 'direct') as version_or_default
    from app_logs
    where hour(log_ts) between 9 and 16
)
-- Aggregate with conditional metrics
select
    date_trunc('hour', log_ts) as hour_window,
    count(distinct region_or_default) as distinct_region_or_defaults,
    min(log_ts) as first_seen,
    min(log_ts) as first_log_ts,
    min(log_ts) as first_log_ts_2,
    (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as duration_min
from flattened_attrs
group by date_trunc('hour', log_ts)
order by duration_min desc nulls first;
