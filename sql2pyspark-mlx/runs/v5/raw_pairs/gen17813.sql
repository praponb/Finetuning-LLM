with lines as (
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) li as k, attr_value
),
valid as (
    select
        id,
        level,
        status_code,
        k,
        concat_ws('-', k, level) as k_level_key,
        upper(host) as host_upper,
        coalesce(k, 'UNKNOWN') as k_clean
    from lines
    where log_ts >= timestamp '2025-06-01 05:00:00'
),
metrics as (
    select
        id,
        level,
        round(status_code / sum(status_code) over (partition by level), 4) as status_code_share,
        sum(status_code) over (partition by level) as level_sum_status_code,
        rank() over (order by status_code desc) as status_code_rank,
        percent_rank() over w_grp as status_code_percent_rank,
        max(status_code) over (partition by level, k) as level_max_status_code
    from valid
    window w_grp as (partition by level order by status_code)
),
metrics_app_logs as (
    select
        id,
        level,
        status_code_share,
        level_sum_status_code,
        status_code_percent_rank,
        level_max_status_code,
        round(level_max_status_code / sum(level_max_status_code) over (partition by level), 4) as level_max_status_code_share,
        count(status_code_percent_rank) over (partition by level) as level_count_status_code_percent_rank
    from metrics
    where status_code_share between 0.36 and 0.77
),
metrics_app_logs_2 as (
    select
        *,
        count(level_count_status_code_percent_rank) over w_recent as level_count_level_count_status_code_percent_rank,
        percent_rank() over (partition by level order by level_count_status_code_percent_rank desc nulls last) as level_count_status_code_percent_rank_percent_rank,
        percent_rank() over w_hist as status_code_percent_rank_percent_rank
    from metrics_app_logs
    window w_recent as (partition by level), w_hist as (partition by level order by status_code_percent_rank nulls last)
),
scored as (
    select
        id,
        status_code_share,
        status_code_percent_rank,
        level_max_status_code,
        level_max_status_code_share,
        level_count_status_code_percent_rank,
        level_count_level_count_status_code_percent_rank,
        sum(status_code_share) over (partition by level) as level_sum_status_code_share,
        percent_rank() over (order by level_count_level_count_status_code_percent_rank) as level_count_level_count_status_code_percent_rank_percent_rank
    from metrics_app_logs_2
),
filtered as (
    select *, level_count_status_code_percent_rank / nullif(level_max_status_code_share, 0) as level_count_status_code_percent_rank_per_level_max_status_code_share
    from scored
    where status_code_percent_rank >= 0.69
)
select
    id,
    status_code_percent_rank,
    level_max_status_code_share,
    level_count_status_code_percent_rank,
    level_count_level_count_status_code_percent_rank,
    level_sum_status_code_share,
    level_count_level_count_status_code_percent_rank_percent_rank
from filtered
order by status_code_percent_rank desc nulls last;
