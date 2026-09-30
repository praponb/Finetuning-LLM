with metrics as (
    -- Running and moving aggregates
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        avg(pressure) over (partition by device_code order by humidity, id rows between 2 preceding and current row) as pressure_avg_2,
        cume_dist() over (order by humidity nulls first) as humidity_cume_dist,
        avg(temp_c) over (order by pressure, id rows between 6 preceding and current row) as temp_c_avg_6
    from device_readings
)
select
    id,
    device_code,
    reading_ts,
    humidity,
    pressure,
    status_bits,
    firmware,
    humidity_cume_dist,
    case when device_code rlike '^(DV-02|DV-03)$' then 'PRIORITY' else 'OTHER_ZONE' end as device_code_zone
from metrics
order by firmware;
