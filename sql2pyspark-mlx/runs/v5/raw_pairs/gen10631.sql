with lines as (
    select
        id,
        log_ts,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) x as k, attr_value
),
metrics as (
    select
        id,
        log_ts,
        status_code,
        k,
        max(status_code) over w_ordered as status_code_max_2h,
        avg(status_code) over (partition by k order by cast(log_ts as bigint) range between 3600 preceding and current row) as status_code_avg_1h
    from lines
    window w_ordered as (order by cast(log_ts as bigint) range between 7200 preceding and current row)
),
cleaned as (
    select
        *,
        status_code + status_code_max_2h as status_code_plus_status_code_max_2h,
        weekofyear(log_ts) as log_ts_weekofyear
    from metrics
    where log_ts >= date '2025-06-01'
),
base as (
    select
        id,
        status_code,
        k,
        status_code_max_2h,
        status_code_avg_1h,
        log_ts_weekofyear,
        status_code_avg_1h + log_ts_weekofyear as status_code_avg_1h_plus_log_ts_weekofyear
    from cleaned
    where k is not null
),
with_windows as (
    select
        *,
        dense_rank() over (partition by k order by log_ts_weekofyear desc nulls first) as log_ts_weekofyear_dense_rank,
        count(status_code) over (partition by k) as k_count_status_code
    from base
    where status_code_max_2h between 286 and 396
),
scored as (
    select
        id,
        k,
        percent_rank() over (partition by k order by status_code_max_2h desc) as status_code_max_2h_percent_rank
    from with_windows
)
select
    case when status_code_max_2h_percent_rank >= 0.77 then 'LARGE' else 'SMALL' end as status_code_max_2h_percent_rank_tier,
    size(collect_set(k)) as num_ks,
    sum(status_code_max_2h_percent_rank) as total_status_code_max_2h_percent_rank,
    max(status_code_max_2h_percent_rank) filter (where k = 'region') as region_max_status_code_max_2h_percent_rank,
    sum(status_code_max_2h_percent_rank) filter (where k = 'version') as version_total_status_code_max_2h_percent_rank
from scored
group by 1
having sum(status_code_max_2h_percent_rank) filter (where k = 'version') >= 0
order by status_code_max_2h_percent_rank_tier desc
