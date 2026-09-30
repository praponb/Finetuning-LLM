with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    from app_logs
    lateral view outer explode(context) e as k, v
),
flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        host,
        log_ts,
        status_code,
        k,
        v,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 8) != 0 as int) as has_fault
    from exploded
    where hour(log_ts) between 7 and 12
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(status_code) over (partition by host) as host_stddev_samp_status_code,
        sum(status_code_high) over (partition by host) as host_sum_status_code_high,
        count(status_code_high) over (partition by host) as host_count_status_code_high
    from flattened_attrs
    where status_code_high < 1
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        log_ts,
        status_code,
        k,
        v,
        has_fault,
        host_stddev_samp_status_code,
        date_format(log_ts, 'MMM yyyy') as log_ts_label
    from metrics
    where k <> 'trace'
),
base as (
    -- Basic cleansing and defaults
    select *, case when status_code >= 423 then 'High' when status_code >= 304 then 'Medium' else 'Low' end as status_code_level
    from valid
    where log_ts >= date '2025-06-01'
      and k is distinct from 'trace'
)
-- Running and moving aggregates
select
    id,
    log_ts,
    k,
    v,
    has_fault,
    host_stddev_samp_status_code,
    log_ts_label,
    percent_rank() over w_recent as status_code_percent_rank,
    percent_rank() over w_grp as host_stddev_samp_status_code_percent_rank,
    round(host_stddev_samp_status_code / sum(host_stddev_samp_status_code) over (partition by k), 4) as host_stddev_samp_status_code_share,
    percent_rank() over w_recent as status_code_percent_rank_2,
    cume_dist() over (partition by k order by has_fault desc nulls last) as has_fault_cume_dist
from base
window w_recent as (partition by k order by status_code asc), w_grp as (partition by k order by host_stddev_samp_status_code desc);
