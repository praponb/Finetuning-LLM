with exploded as (
    select
        id,
        host,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as attr_key, attr_value
),
parsed as (
    select
        id,
        host,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as is_flagged
    from exploded
    where host in ('h1', 'h3')
),
with_windows as (
    select *, rank() over (partition by host order by status_code_high desc nulls last) as status_code_high_rank
    from parsed
),
filtered as (
    select *, is_flagged * status_code_high_rank as is_flagged_times_status_code_high_rank
    from with_windows
    where is_flagged != 1
),
base as (
    select *, status_code_high_rank - is_flagged as status_code_high_rank_minus_is_flagged
    from filtered
    where is_flagged = 1
),
with_windows_app_logs as (
    select
        id,
        is_flagged,
        status_code_high_rank,
        is_flagged_times_status_code_high_rank,
        status_code_high_rank_minus_is_flagged,
        stddev_samp(status_code_high_rank_minus_is_flagged) over (partition by host) as host_stddev_samp_status_code_high_rank_minus_is_flagged
    from base
    where host in ('h1', 'h2')
)
select
    id,
    is_flagged_times_status_code_high_rank,
    host_stddev_samp_status_code_high_rank_minus_is_flagged
from with_windows_app_logs
order by is_flagged_times_status_code_high_rank desc nulls first, host_stddev_samp_status_code_high_rank_minus_is_flagged
