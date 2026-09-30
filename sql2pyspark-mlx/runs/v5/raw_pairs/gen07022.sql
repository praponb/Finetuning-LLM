with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        k,
        value
    from app_logs
    lateral view explode(context) li as k, value
),
scored as (
    -- Running and moving aggregates
    select *, first_value(k) ignore nulls over (partition by k, host order by log_ts, id, k rows between unbounded preceding and current row) as first_known_k
    from lines
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by level, service order by log_ts desc, id, k) as rank_in_group
        from scored
    ) t
    where rank_in_group = 1
),
with_windows as (
    -- Running and moving aggregates
    select *, last_value(k) ignore nulls over (partition by k order by log_ts, id, k rows between unbounded preceding and current row) as last_known_k
    from latest
),
with_windows_app_logs as (
    -- Running and moving aggregates
    select
        id,
        service,
        log_ts,
        k,
        value,
        first_known_k,
        last_known_k,
        row_number() over w_part as row_seq,
        row_number() over w_running as seq_no,
        row_number() over (partition by k order by log_ts, id, k) as row_seq_2,
        first_value(k) ignore nulls over (partition by host, service order by log_ts, id, k rows between unbounded preceding and current row) as first_known_k_2
    from with_windows
    where service in ('search', 'checkout')
    window w_part as (partition by host order by rank_in_group desc, id, k), w_running as (partition by service order by rank_in_group, id, k)
)
select
    id,
    service,
    log_ts,
    k,
    value,
    first_known_k,
    last_known_k,
    row_seq,
    seq_no,
    case when k rlike '^(version|region)$' then 'CORE' when k rlike '^(trace)$' then 'GROUP_B' else 'OTHER_ZONE' end as k_zone
from with_windows_app_logs
order by id nulls first, service;
