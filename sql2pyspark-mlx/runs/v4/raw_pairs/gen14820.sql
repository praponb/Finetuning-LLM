with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        latency_ms,
        key,
        value
    from app_logs
    lateral view explode(context) e as key, value
),
base as (
    -- Keep recent, valid rows
    select *, length(key) as key_length
    from flattened
    where key is not null
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        service,
        latency_ms,
        key,
        key_length,
        ceil(latency_ms) as latency_ms_whole
    from base
    where latency_ms between 1042 and 1504
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(key_length) over w_part as key_length_avg_2,
        sum(latency_ms) over w_part_2 as key_sum_latency_ms
    from valid
    window w_part as (partition by service order by latency_ms_whole, id, key rows between 2 preceding and current row), w_part_2 as (partition by key)
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by key order by latency_ms_whole desc) as rn
        from with_windows
    ) ranked
    where rn = 1
)
select
    id,
    latency_ms,
    key,
    key_length,
    latency_ms_whole,
    key_length_avg_2
from latest
order by latency_ms_whole desc, key desc;
