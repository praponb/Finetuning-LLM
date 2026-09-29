select
    date_trunc('day', reading_ts) as activity_day,
    size(collect_set(device_code)) as num_device_codes,
    count(*) filter (where device_code = 'DV-01') as dv_01_rows,
    sum(case when device_code = 'DV-04' then humidity else 0 end) as dv_04_humidity
from device_readings
group by 1
having sum(case when device_code = 'DV-04' then humidity else 0 end) < 0
order by dv_04_humidity asc;
