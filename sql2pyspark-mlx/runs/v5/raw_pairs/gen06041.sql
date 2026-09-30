with scored as (
    -- Running and moving aggregates
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        lag(temp_c, 1) over (partition by device_code order by reading_ts, id) as prev_temp_c,
        avg(status_bits) over w_running as status_bits_avg_5_prior
    from device_readings
    window w_running as (partition by device_code order by temp_c, id rows between 5 preceding and 1 preceding)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(status_bits_avg_5_prior) ignore nulls over (order by prev_temp_c, id rows between unbounded preceding and current row) as last_known_status_bits_avg_5_prior,
        avg(temp_c) over (partition by device_code order by prev_temp_c, id rows between 6 preceding and 1 preceding) as temp_c_avg_6_prior
    from scored
    where to_date(reading_ts) >= '2023-12-01'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        lag(prev_temp_c, 2) over w_running as prev_prev_temp_c,
        ntile(5) over w_ordered as temp_c_avg_6_prior_bucket,
        dense_rank() over (partition by device_code order by temp_c_avg_6_prior) as temp_c_avg_6_prior_dense_rank,
        min(last_known_status_bits_avg_5_prior) over (partition by device_code) as device_code_min_last_known_status_bits_avg_5_prior,
        row_number() over (order by temp_c desc, id) as seq_no
    from metrics
    window w_running as (order by status_bits_avg_5_prior, id), w_ordered as (partition by device_code order by temp_c_avg_6_prior, id)
),
enriched_device_readings as (
    -- Window metrics over several different partitions
    select
        *,
        avg(prev_prev_temp_c) over (order by status_bits_avg_5_prior, id rows between 6 preceding and 1 preceding) as prev_prev_temp_c_avg_6_prior,
        lead(device_code_min_last_known_status_bits_avg_5_prior, 2) over (order by device_code_min_last_known_status_bits_avg_5_prior, id) as next_device_code_min_last_known_status_bits_avg_5_prior,
        ntile(3) over (partition by device_code order by device_code_min_last_known_status_bits_avg_5_prior desc, id) as device_code_min_last_known_status_bits_avg_5_prior_bucket,
        lead(temp_c_avg_6_prior_bucket, 2) over (partition by device_code order by pressure, id) as next_temp_c_avg_6_prior_bucket
    from enriched
),
totals as (
    -- Grouped metrics
    select
        seq_no,
        max(reading_ts) as ended_at,
        size(collect_set(device_code)) as num_device_codes,
        (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as duration_min,
        count(*) filter (where device_code = 'DV-04') as dv_04_rows,
        count(*) as event_count,
        round(avg(next_temp_c_avg_6_prior_bucket), 2) as avg_next_temp_c_avg_6_prior_bucket
    from enriched_device_readings
    group by seq_no
    having event_count > 1
)
select
    seq_no,
    ended_at,
    num_device_codes,
    duration_min,
    dv_04_rows,
    event_count,
    avg_next_temp_c_avg_6_prior_bucket
from totals
order by dv_04_rows desc
