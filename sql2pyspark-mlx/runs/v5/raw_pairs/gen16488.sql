with with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        reading_ts,
        temp_c,
        firmware,
        count(humidity) over (partition by device_code order by cast(reading_ts as bigint) range between 1800 preceding and current row) as humidity_count_30m,
        lag(status_bits, 1) over (partition by device_code order by reading_ts, id) as prev_status_bits,
        corr(humidity, temp_c) over (partition by device_code) as corr_humidity_temp_c,
        ntile(4) over (partition by device_code order by status_bits, id) as status_bits_bucket
    from device_readings
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, floor(prev_status_bits) as prev_status_bits_whole
    from with_windows
    where reading_ts > timestamp '2025-09-01 09:00:00'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        temp_c,
        firmware,
        humidity_count_30m,
        prev_status_bits,
        corr_humidity_temp_c,
        round(prev_status_bits, 2) as prev_status_bits_rounded
    from filtered
    where hour(reading_ts) between 6 and 13
),
with_windows_device_readings as (
    -- Ranking and gap features
    select
        *,
        max(temp_c) over (order by prev_status_bits, id rows between unbounded preceding and current row) as running_max_temp_c,
        sum(temp_c) over (order by prev_status_bits, id rows between unbounded preceding and current row) as running_sum_temp_c,
        count(temp_c) over (order by prev_status_bits, id rows between unbounded preceding and current row) as running_count_temp_c,
        avg(corr_humidity_temp_c) over (order by prev_status_bits, id rows between 5 preceding and 1 preceding) as corr_humidity_temp_c_avg_5_prior
    from cleaned
    where corr_humidity_temp_c between -0.5 and -0.1
),
cleaned_device_readings as (
    -- Keep recent, valid rows
    select *, prev_status_bits_rounded - prev_status_bits as prev_status_bits_rounded_minus_prev_status_bits
    from with_windows_device_readings
    where humidity_count_30m <> 8
),
with_windows_device_readings_2 as (
    -- Running and moving aggregates
    select
        *,
        lag(prev_status_bits_rounded_minus_prev_status_bits, 1, 0) over (order by prev_status_bits_rounded, id) as prev_prev_status_bits_rounded_minus_prev_status_bits,
        row_number() over w_hist as row_seq,
        sum(running_sum_temp_c) over (order by prev_status_bits_rounded_minus_prev_status_bits, id rows between unbounded preceding and current row) as cumulative_running_sum_temp_c,
        avg(running_sum_temp_c) over (order by corr_humidity_temp_c, id rows between 2 preceding and current row) as running_sum_temp_c_avg_2,
        row_number() over (order by prev_status_bits_rounded_minus_prev_status_bits desc, id) as event_seq
    from cleaned_device_readings
    window w_hist as (order by temp_c desc, id)
)
-- Aggregate with conditional metrics
select
    case when running_sum_temp_c_avg_2 >= 173 then 'PREMIUM' else 'STANDARD' end as running_sum_temp_c_avg_2_tier,
    case when running_sum_temp_c >= 175 then 'LARGE' else 'SMALL' end as running_sum_temp_c_tier,
    count(*) as cnt,
    percentile_approx(row_seq, 0.9) as row_seq_p90,
    min(event_seq) as min_event_seq,
    count(distinct id) as distinct_ids,
    percentile_approx(prev_prev_status_bits_rounded_minus_prev_status_bits, 0.5) as prev_prev_status_bits_rounded_minus_prev_status_bits_p50
from with_windows_device_readings_2
group by rollup (case when running_sum_temp_c_avg_2 >= 173 then 'PREMIUM' else 'STANDARD' end, case when running_sum_temp_c >= 175 then 'LARGE' else 'SMALL' end)
order by min_event_seq desc nulls first;
