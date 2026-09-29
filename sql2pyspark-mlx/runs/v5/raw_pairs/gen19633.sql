with metrics as (
    select
        id,
        humidity,
        pressure,
        min(temp_c) over w_recent as last4_min_temp_c,
        max(temp_c) over w_recent as last4_max_temp_c,
        avg(temp_c) over w_recent as last4_avg_temp_c,
        sum(temp_c) over w_recent as last4_sum_temp_c,
        lag(pressure, 1) over (partition by device_code order by reading_ts, id) as prev_pressure
    from device_readings
    window w_recent as (partition by device_code order by temp_c, id rows between 3 preceding and current row)
)
select
    case when prev_pressure >= 1032 then 'HIGH' else 'LOW' end as prev_pressure_tier,
    sum(last4_max_temp_c) as total_last4_max_temp_c,
    percentile_approx(prev_pressure, array(0.25, 0.5, 0.75)) as prev_pressure_quartiles,
    approx_percentile(last4_sum_temp_c, array(0.25, 0.5, 0.75)) as last4_sum_temp_c_quartiles,
    approx_percentile(last4_max_temp_c, array(0.25, 0.5, 0.75)) as last4_max_temp_c_quartiles,
    avg(last4_max_temp_c) as avg_last4_max_temp_c
from metrics
group by case when prev_pressure >= 1032 then 'HIGH' else 'LOW' end
having avg_last4_max_temp_c < 1000000
order by total_last4_max_temp_c nulls last
