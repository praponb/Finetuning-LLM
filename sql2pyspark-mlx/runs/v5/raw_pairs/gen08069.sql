with filtered as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        cast(status_bits as string) as status_bits_num,
        date_trunc('week', reading_ts) as reading_ts_period
    from device_readings
    where reading_ts >= to_date('2025-09-01')
      and device_code not in ('DV-02', 'DV-01', 'DV-03')
),
filtered_device_readings as (
    select
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits_num,
        reading_ts_period,
        coalesce(humidity, 0) as humidity_filled,
        quarter(reading_ts) as reading_ts_quarter
    from filtered
    where humidity <= 57
),
metrics as (
    select
        id,
        humidity,
        pressure,
        status_bits,
        status_bits_num,
        reading_ts_period,
        lag(humidity_filled) over w_hist as prev_humidity_filled,
        lead(humidity_filled) over w_hist as next_humidity_filled,
        lead(reading_ts_quarter) over w_hist as next_reading_ts_quarter,
        row_number() over w_hist as row_seq
    from filtered_device_readings
    window w_hist as (order by reading_ts_quarter, id)
),
scored as (
    select
        *,
        lag(next_humidity_filled) over w_ordered as prev_next_humidity_filled,
        lag(pressure) over w_ordered as prev_pressure,
        lead(pressure) over w_ordered as next_pressure,
        row_number() over w_ordered as event_seq,
        first_value(humidity, true) over (order by status_bits, id rows between unbounded preceding and current row) as first_known_humidity
    from metrics
    window w_ordered as (order by humidity, id)
),
enriched as (
    select
        id,
        humidity,
        status_bits_num,
        reading_ts_period,
        next_humidity_filled,
        next_reading_ts_quarter,
        row_seq,
        prev_next_humidity_filled,
        prev_pressure,
        next_pressure,
        event_seq,
        lead(humidity) over (order by prev_humidity_filled, id) as next_humidity,
        lag(humidity) over (order by prev_humidity_filled, id) as prev_humidity,
        lag(status_bits) over (order by prev_humidity_filled, id) as prev_status_bits,
        row_number() over (order by prev_humidity_filled, id) as row_seq_2
    from scored
)
select *
from (
    select
        id,
        humidity,
        next_pressure
    from enriched
) src
unpivot (
    val for kpi in (humidity, next_pressure)
);
