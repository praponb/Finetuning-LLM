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
    lateral view outer explode(context) t as key, v
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        level,
        key,
        avg(status_code) over (order by cast(log_ts as bigint) range between 7200 preceding and current row) as status_code_avg_2h,
        max(status_code) over (order by cast(log_ts as bigint) range between 900 preceding and current row) as status_code_max_15m,
        sum(status_code) over (partition by key) as key_sum_status_code,
        rank() over (partition by level, service order by status_code desc) as status_code_rank
    from lines
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        status_code_max_15m,
        key_sum_status_code,
        status_code_rank,
        trim(key) as clean_key,
        floor(status_code_avg_2h) as status_code_avg_2h_whole
    from metrics
    where status_code_avg_2h > 378
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        status_code_max_15m,
        key_sum_status_code,
        clean_key,
        status_code_rank * status_code_max_15m as status_code_rank_times_status_code_max_15m
    from base
    where status_code_avg_2h_whole between 60 and 74
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        status_code_max_15m,
        status_code_rank_times_status_code_max_15m,
        rank() over (order by status_code_rank_times_status_code_max_15m nulls first) as status_code_rank_times_status_code_max_15m_rank,
        dense_rank() over (order by status_code_rank_times_status_code_max_15m nulls last) as status_code_rank_times_status_code_max_15m_dense_rank,
        percent_rank() over (order by status_code_max_15m) as status_code_max_15m_percent_rank
    from filtered
)
-- Grouped metrics
select
    case when status_code_max_15m_percent_rank >= 0.64 then 'HIGH' else 'LOW' end as status_code_max_15m_percent_rank_tier,
    case when status_code_max_15m >= 435 then 'LARGE' else 'SMALL' end as status_code_max_15m_tier,
    count(distinct id) as distinct_ids,
    approx_percentile(status_code_max_15m_percent_rank, array(0.25, 0.5, 0.75)) as status_code_max_15m_percent_rank_quartiles,
    round(avg(status_code_rank_times_status_code_max_15m), 2) as avg_status_code_rank_times_status_code_max_15m,
    grouping_id() as gid
from enriched
group by grouping sets ((case when status_code_max_15m_percent_rank >= 0.64 then 'HIGH' else 'LOW' end, case when status_code_max_15m >= 435 then 'LARGE' else 'SMALL' end), (case when status_code_max_15m_percent_rank >= 0.64 then 'HIGH' else 'LOW' end), ())
having distinct_ids > 1
order by avg_status_code_rank_times_status_code_max_15m desc nulls first
