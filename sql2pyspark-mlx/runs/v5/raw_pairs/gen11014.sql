select
    id,
    temp_c,
    humidity,
    status_bits,
    firmware,
    dense_rank() over (partition by device_code order by humidity desc nulls last) as humidity_dense_rank,
    percent_rank() over (partition by device_code order by status_bits) as status_bits_percent_rank,
    dense_rank() over w_ordered as status_bits_dense_rank,
    avg(pressure) over (partition by device_code order by pressure, id rows between 2 preceding and current row) as pressure_avg_2
from device_readings
window w_ordered as (partition by device_code order by status_bits desc);
