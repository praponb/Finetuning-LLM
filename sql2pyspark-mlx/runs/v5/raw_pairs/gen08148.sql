with parsed as (
    -- Semi-structured parsing
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        context['version'] as version,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        context['trace'] as trace,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count
    from app_logs
    where service = 'auth'
)
-- Window metrics over several different partitions
select
    id,
    log_ts,
    level,
    version,
    user_tier,
    trace,
    retry_count,
    avg(retry_count) over (partition by host order by log_ts, id rows between 6 preceding and current row) as retry_count_avg_6,
    row_number() over w_recent as seq_no,
    row_number() over w_running as row_seq,
    lead(status_code, 1) over w_ordered as next_status_code
from parsed
where retry_count = 2
window w_recent as (partition by level order by log_ts, id), w_running as (partition by host order by log_ts desc, id), w_ordered as (partition by level, host order by status_code, id)
