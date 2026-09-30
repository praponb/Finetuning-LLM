with unpacked as (
    -- Semi-structured parsing
    select
        id,
        service,
        host,
        level,
        payload_json,
        context,
        context['trace'] as trace,
        context['version'] as version,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        get_json_object(payload_json, '$.duration') as duration_text,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec
    from app_logs
    where service in ('search', 'checkout', 'auth')
)
-- Window metrics over several different partitions
select
    id,
    service,
    host,
    level,
    payload_json,
    context,
    trace,
    version,
    user_tier,
    duration_text,
    duration_sec,
    lag(duration_sec, 2) over (partition by level order by duration_sec, id) as prev_duration_sec,
    avg(duration_sec) over (partition by service order by duration_sec, id rows between 6 preceding and current row) as duration_sec_avg_6,
    ntile(3) over (partition by service order by duration_sec, id) as duration_sec_bucket,
    ntile(5) over (partition by host order by duration_sec, id) as duration_sec_bucket_2,
    sum(duration_sec) over (partition by service) as service_sum_duration_sec
from unpacked
