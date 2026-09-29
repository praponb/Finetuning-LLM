with unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        context,
        get_json_object(payload_json, '$.user.id') as user_id,
        array_contains(map_keys(context), 'trace') as has_trace,
        coalesce(context['version'], 'none') as version_or_default,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier
    from app_logs
)
-- Aggregate with conditional metrics
select
    case when latency_ms >= 2520 then 'LARGE' else 'SMALL' end as latency_ms_tier,
    count_if(host = 'h4') as h4_events,
    min(log_ts) as first_seen,
    count(*) filter (where tier = 'pro') as pro_rows,
    grouping(case when latency_ms >= 2520 then 'LARGE' else 'SMALL' end) as gid
from unpacked
group by rollup (case when latency_ms >= 2520 then 'LARGE' else 'SMALL' end)
order by pro_rows desc
