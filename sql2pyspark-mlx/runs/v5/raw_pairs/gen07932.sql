with cleaned as (
    select
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        initcap(trim(device_code)) as device_code_title
    from device_readings
    where reading_ts >= timestamp '2025-09-01 04:00:00'
      and temp_c between 30 and 31
),
cleaned_device_readings as (
    select
        *,
        cast(reading_ts as date) as reading_ts_day,
        round(humidity, 1) as humidity_rounded
    from cleaned
    where reading_ts >= to_date('2025-09-01')
      and hour(reading_ts) between 6 and 11
),
enriched as (
    select
        *,
        lead(humidity_rounded) over w_hist as next_humidity_rounded,
        lead(temp_c) over w_hist as next_temp_c,
        row_number() over w_hist as seq_no,
        first_value(temp_c, true) over w_grp as first_known_temp_c
    from cleaned_device_readings
    window w_hist as (order by humidity, id), w_grp as (order by status_bits, id rows between unbounded preceding and current row)
),
valid as (
    select
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        device_code_title,
        reading_ts_day,
        humidity_rounded,
        next_humidity_rounded,
        next_temp_c,
        first_known_temp_c,
        cast(first_known_temp_c as int) as first_known_temp_c_int
    from enriched
    where next_humidity_rounded between 45 and 64
),
with_windows as (
    select
        *,
        last_value(first_known_temp_c, true) over w_part as last_known_first_known_temp_c,
        sum(next_humidity_rounded) over (order by reading_ts_day, id rows between unbounded preceding and current row) as running_next_humidity_rounded,
        row_number() over w_running as event_seq,
        lag(humidity_rounded, 1) over (order by next_humidity_rounded, id) as prev_humidity_rounded
    from valid
    window w_part as (order by reading_ts, id rows between unbounded preceding and current row), w_running as (order by humidity_rounded desc, id)
),
enriched_device_readings as (
    select
        id,
        reading_ts,
        pressure,
        device_code_title,
        last_known_first_known_temp_c,
        event_seq,
        lag(next_temp_c, 2) over (order by pressure, id) as prev_next_temp_c,
        first_value(next_temp_c, true) over (order by first_known_temp_c, id rows between unbounded preceding and current row) as first_known_next_temp_c,
        dense_rank() over (order by last_known_first_known_temp_c) as last_known_first_known_temp_c_dense_rank
    from with_windows
    where first_known_temp_c >= 22
),
scored as (
    select
        id,
        reading_ts,
        last_known_first_known_temp_c,
        prev_next_temp_c,
        last_known_first_known_temp_c_dense_rank,
        cume_dist() over (order by last_known_first_known_temp_c_dense_rank nulls first) as last_known_first_known_temp_c_dense_rank_cume_dist,
        avg(event_seq) over (order by pressure, id rows between 5 preceding and 1 preceding) as event_seq_avg_5_prior
    from enriched_device_readings
)
select
    date_trunc('day', reading_ts) as activity_day,
    date_trunc('hour', reading_ts) as hour_window,
    case when prev_next_temp_c >= 41.3 then 'PREMIUM' else 'STANDARD' end as prev_next_temp_c_tier,
    sum(event_seq_avg_5_prior) as total_event_seq_avg_5_prior,
    count(*) as num_rows
from scored
group by date_trunc('day', reading_ts), date_trunc('hour', reading_ts), case when prev_next_temp_c >= 41.3 then 'PREMIUM' else 'STANDARD' end
order by num_rows desc;
