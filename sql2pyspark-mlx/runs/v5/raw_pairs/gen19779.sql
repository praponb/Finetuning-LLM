with lines as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view explode(context) x as key, attr_value
),
cleaned as (
    select *, case when level in ('DEBUG', 'ERROR') then 'Tier 1' else 'Other' end as level_group
    from lines
    where log_ts >= date '2025-06-01'
      and key is not null
),
metrics as (
    select
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        attr_value,
        level_group,
        cume_dist() over (partition by key order by latency_ms asc nulls last) as latency_ms_cume_dist,
        sum(latency_ms) over (partition by key order by latency_ms asc, id asc, key asc rows between unbounded preceding and current row) as running_latency_ms
    from cleaned
),
metrics_app_logs as (
    select
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        attr_value,
        running_latency_ms,
        sum(running_latency_ms) over (order by latency_ms_cume_dist asc, id asc, key asc rows between unbounded preceding and current row) as running_running_latency_ms,
        percent_rank() over (partition by key order by status_code asc) as status_code_percent_rank,
        first_value(latency_ms_cume_dist, true) over (order by status_code asc, id asc, key asc rows between unbounded preceding and current row) as first_known_latency_ms_cume_dist,
        ntile(10) over (partition by host order by running_latency_ms desc, id asc, key asc) as running_latency_ms_bucket
    from metrics
    where latency_ms_cume_dist >= 0.59
),
with_windows as (
    select
        id,
        host,
        log_ts,
        status_code,
        key,
        attr_value,
        running_latency_ms,
        running_running_latency_ms,
        status_code_percent_rank,
        first_known_latency_ms_cume_dist,
        row_number() over w_hist as seq_no,
        lag(status_code_percent_rank, 1) over w_recent as prev_status_code_percent_rank
    from metrics_app_logs
    window w_hist as (partition by host order by first_known_latency_ms_cume_dist asc, id asc, key asc), w_recent as (partition by key order by running_latency_ms_bucket asc, id asc, key asc)
),
with_windows_app_logs as (
    select
        id,
        status_code,
        key,
        attr_value,
        running_latency_ms,
        first_known_latency_ms_cume_dist,
        seq_no,
        sum(running_latency_ms) over w_grp as cumulative_running_latency_ms,
        last_value(seq_no, true) over (partition by host order by log_ts asc, id asc, key asc rows between unbounded preceding and current row) as last_known_seq_no
    from with_windows
    window w_grp as (partition by key order by seq_no asc, id asc, key asc rows between unbounded preceding and current row)
),
valid as (
    select
        id,
        status_code,
        key,
        running_latency_ms,
        seq_no,
        cumulative_running_latency_ms,
        cast(running_latency_ms as string) as running_latency_ms_num
    from with_windows_app_logs
    where key is not null
)
select
    id,
    status_code,
    key,
    cumulative_running_latency_ms,
    case when key rlike '^(version|trace)$' then 'OTHER_ZONE' else 'CORE' end as key_zone
from valid
order by status_code desc;
