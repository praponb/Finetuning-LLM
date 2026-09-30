with with_windows as (
    select
        *,
        lead(pressure) over (partition by device_code order by reading_ts asc, id asc) as next_pressure,
        lag(pressure) over (partition by device_code order by reading_ts asc, id asc) as prev_pressure,
        lag(status_bits) over (partition by device_code order by reading_ts asc, id asc) as prev_status_bits,
        lead(status_bits) over (partition by device_code order by reading_ts asc, id asc) as next_status_bits,
        row_number() over (partition by device_code order by reading_ts asc, id asc) as row_seq
    from device_readings
),
enriched as (
    select
        *,
        avg(pressure) over w_running as running_avg_pressure,
        min(pressure) over w_running as running_min_pressure,
        count(pressure) over w_running as running_count_pressure,
        avg(next_pressure) over (order by next_pressure asc, id asc rows between 2 preceding and current row) as next_pressure_avg_2
    from with_windows
    where row_seq > 6
    window w_running as (partition by device_code order by prev_status_bits asc, id asc rows between unbounded preceding and current row)
),
matched_keys as (
    select id, firmware
    from enriched
    intersect
    select id, firmware
    from device_readings
    where status_bits >= 8
),
verified as (
    select s.*
    from enriched s
    inner join matched_keys k on s.id = k.id and s.firmware = k.firmware
),
enriched_device_readings as (
    select *, max(next_pressure_avg_2) over (partition by device_code order by cast(reading_ts as bigint) asc range between 3600 preceding and current row) as next_pressure_avg_2_max_1h
    from verified
),
filtered as (
    select *, prev_status_bits + status_bits as prev_status_bits_plus_status_bits
    from enriched_device_readings
    where firmware = '1.1.0'
),
stats as (
    select
        device_code,
        count_if(device_code = 'DV-02') as dv_02_events,
        percentile_approx(running_count_pressure, 0.9) as running_count_pressure_p90,
        count(*) filter (where device_code = 'DV-04') as dv_04_rows
    from filtered
    group by rollup (device_code)
)
select
    device_code,
    dv_04_rows,
    case when device_code rlike '^(DV-02|DV-01)$' then 'GROUP_B' else 'OTHER_ZONE' end as device_code_zone
from stats
order by device_code_zone asc
