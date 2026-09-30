with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) li as key, attr_value
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        max(status_code) over (partition by level, key) as level_max_status_code,
        avg(status_code) over (partition by level order by cast(log_ts as bigint) range between 900 preceding and current row) as status_code_avg_15m
    from lines
),
scored as (
    -- Ranking and gap features
    select
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        attr_value,
        level_max_status_code,
        status_code_avg_15m,
        count(status_code) over w_running as status_code_count_1h,
        avg(status_code) over w_part as status_code_avg_15m_2,
        count(status_code_avg_15m) over w_recent as status_code_avg_15m_count_30m,
        round(status_code_avg_15m / sum(status_code_avg_15m) over (partition by level), 4) as status_code_avg_15m_share
    from enriched
    window w_running as (partition by level order by cast(log_ts as bigint) range between 3600 preceding and current row), w_part as (partition by key order by cast(log_ts as bigint) range between 900 preceding and current row), w_recent as (partition by service order by cast(log_ts as bigint) range between 1800 preceding and current row)
)
select
    id,
    service,
    log_ts,
    level,
    key,
    attr_value,
    level_max_status_code,
    status_code_avg_15m,
    status_code_count_1h,
    case when key rlike '^(version|trace)$' then 'CORE' else 'GROUP_A' end as key_zone
from scored
order by log_ts nulls first
