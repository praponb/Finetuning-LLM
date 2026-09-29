with unpacked as (
    select
        id,
        log_ts,
        status_code,
        payload_json,
        context,
        cast((status_code & 1) != 0 as int) as is_flagged,
        context['region'] as region,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count
    from app_logs
)
select
    id,
    log_ts,
    status_code,
    is_flagged,
    retry_count,
    max(status_code) over (order by retry_count, id rows between 3 preceding and current row) as last4_max_status_code,
    count(status_code) over (order by retry_count, id rows between 3 preceding and current row) as last4_count_status_code,
    sum(status_code) over (order by retry_count, id rows between 3 preceding and current row) as last4_sum_status_code,
    avg(status_code) over (order by retry_count, id rows between 3 preceding and current row) as last4_avg_status_code
from unpacked
where is_flagged > 0
