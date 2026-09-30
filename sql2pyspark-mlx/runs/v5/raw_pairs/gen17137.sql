with valid as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        concat_ws('/', device_code, firmware) as device_code_firmware_key,
        cast(temp_c as int) as temp_c_int
    from device_readings
    where reading_ts > date '2025-09-01'
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        reading_ts,
        pressure,
        temp_c_int,
        lag(status_bits) over w_ordered as prev_status_bits,
        lag(temp_c) over w_ordered as prev_temp_c,
        lead(temp_c) over w_ordered as next_temp_c,
        row_number() over w_ordered as event_seq,
        count(temp_c_int) over w_part as temp_c_int_count_1h
    from valid
    where status_bits < 8
    window w_ordered as (partition by device_code order by temp_c_int, id), w_part as (partition by device_code order by cast(reading_ts as bigint) range between 3600 preceding and current row)
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        temp_c_int,
        next_temp_c,
        ntile(4) over w_running as pressure_bucket,
        sum(pressure) over (order by cast(reading_ts as bigint) range between 7200 preceding and current row) as pressure_sum_2h,
        avg(temp_c_int_count_1h) over w_part as temp_c_int_count_1h_avg_3
    from scored
    window w_running as (order by pressure, id), w_part as (order by temp_c_int_count_1h, id rows between 3 preceding and current row)
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        temp_c_int_count_1h_avg_3,
        avg(pressure_sum_2h) over w_ordered as last7_avg_pressure_sum_2h,
        count(pressure_sum_2h) over w_ordered as last7_count_pressure_sum_2h,
        sum(pressure_sum_2h) over w_ordered as last7_sum_pressure_sum_2h
    from with_windows
    window w_ordered as (order by temp_c_int_count_1h_avg_3, id rows between 6 preceding and current row)
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        temp_c_int_count_1h_avg_3,
        last7_avg_pressure_sum_2h,
        last7_count_pressure_sum_2h,
        last7_count_pressure_sum_2h * last7_sum_pressure_sum_2h as last7_count_pressure_sum_2h_times_last7_sum_pressure_sum_2h
    from metrics
    where last7_sum_pressure_sum_2h < 16000
)
-- Multi-level aggregation
select
    case when last7_count_pressure_sum_2h >= 13 then 'PREMIUM' else 'STANDARD' end as last7_count_pressure_sum_2h_tier,
    case when temp_c_int_count_1h_avg_3 >= 15 then 'HIGH' else 'LOW' end as temp_c_int_count_1h_avg_3_tier,
    case when last7_avg_pressure_sum_2h >= 4200 then 'HIGH' else 'LOW' end as last7_avg_pressure_sum_2h_tier,
    count(*) as cnt,
    approx_percentile(temp_c_int_count_1h_avg_3, array(0.25, 0.5, 0.75)) as temp_c_int_count_1h_avg_3_quartiles,
    grouping(case when last7_count_pressure_sum_2h >= 13 then 'PREMIUM' else 'STANDARD' end) as grouping_id
from filtered
group by rollup (case when last7_count_pressure_sum_2h >= 13 then 'PREMIUM' else 'STANDARD' end, case when temp_c_int_count_1h_avg_3 >= 15 then 'HIGH' else 'LOW' end, case when last7_avg_pressure_sum_2h >= 4200 then 'HIGH' else 'LOW' end)
having count(*) < 1000
order by temp_c_int_count_1h_avg_3_tier desc;
