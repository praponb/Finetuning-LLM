with metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(temp_c) over (partition by device_code order by status_bits, id rows between unbounded preceding and current row) as running_temp_c,
        avg(status_bits) over (partition by device_code order by humidity, id rows between 3 preceding and 1 preceding) as status_bits_avg_3_prior
    from device_readings
),
ranked as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by device_code order by status_bits_avg_3_prior nulls last) as rank_in_group
    from metrics
    qualify rank_in_group = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        running_temp_c,
        status_bits_avg_3_prior,
        rank_in_group,
        ntile(4) over w_ordered as running_temp_c_bucket,
        last_value(running_temp_c) ignore nulls over w_grp as last_known_running_temp_c,
        avg(temp_c) over (partition by device_code order by humidity, id rows between 5 preceding and current row) as temp_c_avg_5,
        cume_dist() over w_part as pressure_cume_dist,
        last_value(humidity, true) over (order by status_bits_avg_3_prior, id rows between unbounded preceding and current row) as last_known_humidity
    from ranked
    window w_ordered as (partition by device_code order by running_temp_c desc, id), w_grp as (order by pressure, id rows between unbounded preceding and current row), w_part as (partition by device_code order by pressure nulls first)
),
with_prev as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from scored
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval 15 minutes then 1 else 0 end as new_group_flag
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from gaps
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        ceil(last_known_running_temp_c) as last_known_running_temp_c_whole,
        temp_c_avg_5 - pressure as temp_c_avg_5_minus_pressure
    from islands
    where status_bits_avg_3_prior >= 10
),
gaps_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when prev_reading_ts - lag(prev_reading_ts) over (partition by device_code order by prev_reading_ts, id) > interval 14 days then 1 else 0 end as session_start_flag
    from base
),
islands_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device_code order by prev_reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps_2
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from islands_device_readings
    qualify row_number() over (partition by device_code order by status_bits_avg_3_prior desc nulls first, id) = 1
)
-- Grouped metrics
select
    case when session_start_flag >= 1 then 'LARGE' else 'SMALL' end as session_start_flag_tier,
    case when burst_id >= 6 then 'HIGH' else 'LOW' end as burst_id_tier,
    (unix_timestamp(max(prev_reading_ts)) - unix_timestamp(min(prev_reading_ts))) / 60 as duration_min,
    count_if(device_code = 'DV-02') as dv_02_events,
    min(prev_reading_ts) as started_at,
    count(case when device_code = 'DV-03' then 1 end) as dv_03_count
from deduped
group by 1, 2
having (unix_timestamp(max(prev_reading_ts)) - unix_timestamp(min(prev_reading_ts))) / 60 < 0
