with parsed as (
    -- Semi-structured parsing
    select
        id,
        host,
        level,
        latency_ms,
        context,
        context['version'] as version,
        context['trace'] as trace,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec
    from app_logs
    where host not in ('h4', 'h1', 'h3')
)
-- Multi-level aggregation
select
    host,
    bit_or(latency_ms) as latency_ms_bit_or,
    count(*) as num_rows,
    count(*) as row_count,
    bit_and(latency_ms) as latency_ms_bit_and,
    sum(duration_sec) filter (where level = 'WARN') as warn_total_duration_sec
from parsed
group by 1
order by latency_ms_bit_or desc
