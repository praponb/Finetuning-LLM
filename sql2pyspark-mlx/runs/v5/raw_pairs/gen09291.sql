with flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        service,
        host,
        log_ts,
        level,
        context['trace'] as trace,
        shiftright(status_code, 2) as status_code_high,
        array_contains(map_keys(context), 'version') as has_version,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier
    from app_logs
)
-- Ranking and gap features
select
    id,
    host,
    level,
    min(status_code_high) over (partition by service, level order by log_ts, id rows between unbounded preceding and current row) as running_min_status_code_high,
    count(status_code_high) over (partition by service, level order by log_ts, id rows between unbounded preceding and current row) as running_count_status_code_high,
    max(status_code_high) over (partition by service, level order by log_ts, id rows between unbounded preceding and current row) as running_max_status_code_high
from flattened_attrs
where tier = 'gold'
