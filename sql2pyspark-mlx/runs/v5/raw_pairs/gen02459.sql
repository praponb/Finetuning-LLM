with base as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        case when device_code in ('DV-02', 'DV-01') then 'Core' else 'Other' end as device_code_group
    from device_readings
    where reading_ts >= date '2025-09-01'
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) > interval '30' minute then 1 else 0 end as new_group_flag
    from base
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        humidity,
        sum(new_group_flag) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as session_no
    from flagged
),
metrics as (
    -- Ranking and gap features
    select
        id,
        device_code,
        humidity,
        session_no,
        sum(session_no) over w_hist as running_sum_session_no,
        min(session_no) over w_hist as running_min_session_no,
        avg(session_no) over w_hist as running_avg_session_no,
        row_number() over w_hist_2 as row_seq
    from sessionized
    where humidity between 55 and 64
    window w_hist as (partition by device_code order by humidity asc, id asc rows between unbounded preceding and current row), w_hist_2 as (partition by device_code order by humidity asc, id asc)
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        humidity,
        session_no,
        running_sum_session_no,
        running_min_session_no,
        running_avg_session_no,
        row_seq,
        session_no - running_sum_session_no as session_no_minus_running_sum_session_no
    from metrics
    where running_sum_session_no < 36
),
filtered_device_readings as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(humidity, 0) as humidity_filled,
        coalesce(session_no_minus_running_sum_session_no, 0) as session_no_minus_running_sum_session_no_filled
    from filtered
    where humidity <= 73
),
scored as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (order by running_min_session_no desc, id asc) as row_seq_2,
        avg(running_sum_session_no) over w_running as running_sum_session_no_avg_3,
        avg(row_seq) over (partition by device_code order by humidity asc, id asc rows between 3 preceding and 1 preceding) as row_seq_avg_3_prior
    from filtered_device_readings
    window w_running as (partition by device_code order by running_avg_session_no asc, id asc rows between 3 preceding and current row)
)
-- Multi-level aggregation
select
    device_code,
    case when running_avg_session_no >= 8.0 then 'HIGH' else 'LOW' end as running_avg_session_no_tier,
    avg(humidity_filled) filter (where device_code = 'DV-04') as dv_04_avg_humidity_filled,
    percentile_approx(running_avg_session_no, array(0.25, 0.5, 0.75)) as running_avg_session_no_quartiles,
    sort_array(collect_set(device_code)) as device_code_list
from scored
group by device_code, running_avg_session_no_tier
order by running_avg_session_no_tier asc nulls last
