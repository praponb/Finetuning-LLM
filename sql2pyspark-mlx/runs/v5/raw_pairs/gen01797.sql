with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        cast(temp_c as int) as temp_c_int,
        lower(device_code) as device_code_lower
    from device_readings
    where reading_ts >= timestamp '2025-09-01 07:00:00'
      and reading_ts >= '2025-03-01'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        temp_c,
        pressure,
        count(temp_c) over w_running as running_count_temp_c,
        min(temp_c) over w_running as running_min_temp_c,
        sum(temp_c) over w_running as running_sum_temp_c,
        avg(temp_c) over w_running as running_avg_temp_c,
        ntile(4) over w_grp as status_bits_bucket
    from valid
    where status_bits = 12
    window w_running as (order by temp_c, id rows between unbounded preceding and current row), w_grp as (order by status_bits, id)
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        running_min_temp_c,
        running_sum_temp_c,
        running_avg_temp_c,
        dense_rank() over w_recent as temp_c_dense_rank,
        row_number() over w_grp as event_seq,
        avg(temp_c) over (order by running_count_temp_c, id rows between 6 preceding and current row) as temp_c_avg_6,
        row_number() over w_grp as row_seq
    from with_windows
    window w_recent as (order by temp_c desc), w_grp as (order by status_bits_bucket desc, id)
),
base as (
    -- Keep recent, valid rows
    select
        id,
        running_avg_temp_c,
        temp_c_avg_6,
        row_seq,
        case when running_sum_temp_c >= 147 then 'High' when running_sum_temp_c >= 49 then 'Medium' else 'Low' end as running_sum_temp_c_band
    from enriched
    where row_seq >= 3
)
select
    id,
    running_avg_temp_c,
    temp_c_avg_6,
    running_sum_temp_c_band
from base
order by id desc nulls last;
