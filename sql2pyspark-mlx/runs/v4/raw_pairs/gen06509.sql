with filtered as (
    select
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        round(temp_c * 1.07, 2) temp_c_scaled,
        year(reading_ts) reading_ts_year
    from device_readings
    where reading_ts > date '2025-09-01'
),
deduped as (
    select *, row_number() over (partition by device_code order by humidity, id) as row_num
    from filtered
    qualify row_num = 1
),
enriched as (
    select
        *,
        rank() over w_running humidity_rank,
        avg(pressure) over (partition by device_code order by pressure, id rows between 5 preceding and current row) pressure_avg_5,
        min(humidity) over (partition by device_code) device_code_min_humidity
    from deduped
    window w_running as (partition by device_code order by humidity desc nulls first)
)
select
    date_trunc('hour', reading_ts) as hour_window,
    max(reading_ts) as last_seen,
    sum(humidity_rank) filter (where device_code = 'DV-02') as dv_02_total_humidity_rank,
    sum(case when device_code = 'DV-03' then pressure_avg_5 else 0 end) as dv_03_pressure_avg_5,
    count(*) as row_count
from enriched
group by hour_window
having dv_03_pressure_avg_5 > 0
order by dv_03_pressure_avg_5 desc nulls last
