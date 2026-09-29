with valid as (
    select
        id,
        temp_c,
        humidity,
        pressure,
        firmware,
        case when device_code in ('DV-02', 'DV-03') then 'Core' else 'Other' end as device_code_group,
        round(pressure, 2) as pressure_rounded
    from device_readings
    where device_code not in ('DV-01', 'DV-03')
),
cleaned as (
    select *, humidity * temp_c as humidity_times_temp_c
    from valid
    where temp_c > 38
),
scored as (
    select
        id,
        humidity,
        device_code_group,
        count(temp_c) over w_part as running_count_temp_c,
        min(temp_c) over w_part as running_min_temp_c,
        avg(temp_c) over w_part as running_avg_temp_c,
        ntile(4) over (order by temp_c desc, id asc) as temp_c_bucket
    from cleaned
    window w_part as (order by temp_c asc, id asc rows between unbounded preceding and current row)
),
metrics as (
    select
        id,
        device_code_group,
        running_min_temp_c,
        max(running_min_temp_c) over w_hist as last7_max_running_min_temp_c,
        min(running_min_temp_c) over w_hist as last7_min_running_min_temp_c,
        avg(running_min_temp_c) over w_hist as last7_avg_running_min_temp_c,
        lag(running_avg_temp_c, 2) over (order by humidity asc, id asc) as prev_running_avg_temp_c
    from scored
    window w_hist as (order by running_min_temp_c asc, id asc rows between 6 preceding and current row)
),
enriched as (
    select
        *,
        lead(prev_running_avg_temp_c) over w_ordered as next_prev_running_avg_temp_c,
        lag(prev_running_avg_temp_c) over w_ordered as prev_prev_running_avg_temp_c,
        lead(last7_avg_running_min_temp_c) over w_ordered as next_last7_avg_running_min_temp_c,
        lag(last7_avg_running_min_temp_c) over w_ordered as prev_last7_avg_running_min_temp_c,
        row_number() over w_ordered as event_seq,
        first_value(last7_avg_running_min_temp_c, true) over (order by last7_avg_running_min_temp_c asc, id asc rows between unbounded preceding and current row) as first_known_last7_avg_running_min_temp_c
    from metrics
    window w_ordered as (order by running_min_temp_c asc, id asc)
)
select
    id,
    device_code_group,
    last7_max_running_min_temp_c,
    last7_min_running_min_temp_c,
    last7_avg_running_min_temp_c,
    prev_running_avg_temp_c,
    prev_prev_running_avg_temp_c,
    prev_last7_avg_running_min_temp_c,
    event_seq
from enriched
order by last7_max_running_min_temp_c asc nulls first
