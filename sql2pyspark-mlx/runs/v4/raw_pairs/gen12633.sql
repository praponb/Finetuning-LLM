with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        v
    from app_logs
    lateral view outer explode(context) x as key, v
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
        v,
        dense_rank() over (partition by key order by status_code) as status_code_dense_rank,
        sum(status_code) over w_hist as level_sum_status_code,
        percent_rank() over w_running as status_code_percent_rank,
        count(status_code) over (order by cast(log_ts as bigint) range between 1800 preceding and current row) as status_code_count_30m,
        max(status_code) over (partition by level, service) as level_max_status_code
    from lines
    window w_hist as (partition by level, key), w_running as (order by status_code desc)
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        upper(service) as service_upper,
        cast(status_code as double) as status_code_str,
        coalesce(key, 'N/A') as key_clean
    from scored
    where level_sum_status_code > 1402
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (partition by level order by status_code_percent_rank nulls first) as status_code_percent_rank_cume_dist,
        rank() over (partition by service order by level_max_status_code desc nulls first) as level_max_status_code_rank,
        dense_rank() over (partition by service order by status_code_count_30m) as status_code_count_30m_dense_rank
    from filtered
)
select
    id,
    service,
    log_ts,
    level,
    status_code,
    level_sum_status_code,
    status_code_count_30m,
    service_upper,
    key_clean,
    case when level rlike '^(INFO|ERROR)$' then 'GROUP_B' else 'GROUP_A' end as level_zone
from metrics
