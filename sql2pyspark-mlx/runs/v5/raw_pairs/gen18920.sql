with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    from app_logs
    lateral view explode(context) x as k, v
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        host,
        level,
        k,
        v,
        row_number() over (partition by host order by log_ts, id, k) row_seq,
        row_number() over (partition by service, host order by log_ts, id, k) row_seq_2
    from exploded
    where status_code between 290 and 316
),
valid as (
    -- Keep recent, valid rows
    select *, case when level = 'DEBUG' then 1 else 0 end is_debug
    from metrics
    where row_seq < 3
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        level,
        k,
        row_seq_2,
        is_debug,
        case when k in ('trace', 'version') then 'Priority' else 'Other' end k_group,
        coalesce(k, 'DIRECT') resolved_k
    from valid
    where k is not null
),
filtered_app_logs as (
    -- Basic cleansing and defaults
    select *, case when is_debug >= 82 then 'High' when is_debug >= 25 then 'Medium' else 'Low' end is_debug_band
    from filtered
    where is_debug = 78
)
-- Aggregate with conditional metrics
select
    case when row_seq_2 >= 6 then 'HIGH' else 'LOW' end as row_seq_2_tier,
    avg(row_seq_2) filter (where k = 'trace') as trace_avg_row_seq_2,
    max(row_seq_2) filter (where level = 'WARN') as warn_max_row_seq_2
from filtered_app_logs
group by case when row_seq_2 >= 6 then 'HIGH' else 'LOW' end
order by warn_max_row_seq_2 desc;
