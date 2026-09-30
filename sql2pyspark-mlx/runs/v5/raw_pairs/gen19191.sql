with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) x as key, attr_value
),
parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        level,
        latency_ms,
        status_code,
        key,
        attr_value,
        cast((status_code & 2) != 0 as int) as flag_bit2,
        shiftright(status_code, 2) as status_code_high
    from exploded
    where status_code = 368
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, latency_ms - flag_bit2 as latency_ms_minus_flag_bit2
    from parsed
    where key is not null
      and key in ('region', 'version')
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(latency_ms) over (partition by level) as level_stddev_samp_latency_ms,
        dense_rank() over w_running as status_code_dense_rank,
        dense_rank() over (partition by level order by latency_ms desc) as latency_ms_dense_rank,
        rank() over (partition by level order by flag_bit2 nulls first) as flag_bit2_rank
    from valid
    where key = 'region'
    window w_running as (partition by level order by status_code)
)
select
    level,
    latency_ms,
    key,
    attr_value,
    flag_bit2,
    status_code_high,
    latency_ms_minus_flag_bit2,
    level_stddev_samp_latency_ms,
    latency_ms_dense_rank
from with_windows
order by level nulls first;
