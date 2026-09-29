with parsed as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        context,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
        get_json_object(payload_json, '$.user.id') as user_id,
        context['trace'] as trace,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier
    from app_logs
)
select
    *,
    last_value(tier, true) over w_recent as last_known_tier,
    sum(duration_sec) over (partition by service, level order by latency_ms, id rows between unbounded preceding and current row) as running_duration_sec
from parsed
where duration_sec < 2.1
window w_recent as (partition by host order by latency_ms, id rows between unbounded preceding and current row);
