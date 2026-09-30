with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        v
    from app_logs
    lateral view outer explode(context) li as key, v
),
parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        service,
        host,
        log_ts,
        status_code,
        key,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as has_fault
    from exploded
),
matched_keys as (
    -- Set difference on keys
    select host
    from parsed
    except
    select host
    from app_logs
    where level in ('ERROR', 'INFO', 'DEBUG')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from parsed s
    join matched_keys k on s.host = k.host
),
metrics as (
    -- Ranking and gap features
    select
        host,
        log_ts,
        status_code,
        key,
        count(status_code_high) over (partition by host) as host_count_status_code_high
    from reconciled
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        round(status_code / sum(status_code) over (partition by host), 4) as status_code_share,
        avg(host_count_status_code_high) over (partition by host order by cast(log_ts as bigint) range between 900 preceding and current row) as host_count_status_code_high_avg_15m
    from metrics
),
enriched_app_logs as (
    -- Running and moving aggregates
    select
        host,
        log_ts,
        host_count_status_code_high,
        status_code_share,
        host_count_status_code_high_avg_15m,
        avg(host_count_status_code_high) over (partition by host order by cast(log_ts as bigint) range between 3600 preceding and current row) as host_count_status_code_high_avg_1h,
        round(status_code / sum(status_code) over (partition by key, host), 4) as status_code_share_2
    from enriched
),
scored as (
    -- Ranking and gap features
    select
        host,
        log_ts,
        status_code_share,
        host_count_status_code_high_avg_15m,
        host_count_status_code_high_avg_1h,
        status_code_share_2,
        sum(host_count_status_code_high_avg_15m) over (order by cast(log_ts as bigint) range between 3600 preceding and current row) as host_count_status_code_high_avg_15m_sum_1h
    from enriched_app_logs
)
select
    log_ts,
    status_code_share,
    host_count_status_code_high_avg_15m,
    status_code_share_2,
    host_count_status_code_high_avg_15m_sum_1h
from scored
order by log_ts desc nulls first
