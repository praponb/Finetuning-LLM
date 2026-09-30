with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) e as k, value
),
parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        log_ts,
        k,
        cast((status_code & 2) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from exploded
    where log_ts >= '2023-07-13'
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        k,
        max(is_flagged) over (partition by k order by log_ts, id, k rows between unbounded preceding and current row) as running_max_is_flagged,
        avg(is_flagged) over (partition by k order by log_ts, id, k rows between unbounded preceding and current row) as running_avg_is_flagged,
        count(is_flagged) over (partition by k order by log_ts, id, k rows between unbounded preceding and current row) as running_count_is_flagged
    from parsed
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lag(running_avg_is_flagged) over (partition by k order by running_avg_is_flagged, id, k) as prev_running_avg_is_flagged,
        lead(running_avg_is_flagged) over (partition by k order by running_avg_is_flagged, id, k) as next_running_avg_is_flagged,
        lead(running_count_is_flagged) over (partition by k order by running_avg_is_flagged, id, k) as next_running_count_is_flagged,
        row_number() over (partition by k order by running_avg_is_flagged, id, k) as seq_no
    from metrics
    where k in ('trace', 'version', 'region')
),
metrics_app_logs as (
    -- Running and moving aggregates
    select
        *,
        ntile(10) over (partition by k order by running_max_is_flagged, id, k) as running_max_is_flagged_bucket,
        sum(seq_no) over (partition by k order by running_max_is_flagged, id, k rows between unbounded preceding and current row) as running_seq_no
    from with_windows
)
-- Grouped metrics
select
    seq_no,
    approx_percentile(next_running_avg_is_flagged, 0.5) as next_running_avg_is_flagged_p50,
    count(*) filter (where k = 'region') as region_rows,
    approx_percentile(running_max_is_flagged, 0.9) as running_max_is_flagged_p90,
    size(collect_set(k)) as num_ks
from metrics_app_logs
group by seq_no
order by region_rows nulls last
