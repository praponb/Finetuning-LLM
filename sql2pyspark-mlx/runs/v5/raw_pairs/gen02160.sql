with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        floor(status_bits) as status_bits_whole
    from device_readings
    where reading_ts > timestamp '2025-09-01 06:00:00'
      and temp_c <= 24
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lag(status_bits) over (partition by device_code order by reading_ts, id) as prev_status_bits,
        lead(temp_c) over (partition by device_code order by reading_ts, id) as next_temp_c,
        row_number() over (partition by device_code order by reading_ts, id) as seq_no,
        sum(status_bits) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as running_status_bits
    from filtered
    where status_bits_whole = 40
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        device_code,
        temp_c,
        humidity,
        firmware,
        prev_status_bits,
        next_temp_c,
        running_status_bits,
        percent_rank() over w_recent as next_temp_c_percent_rank,
        avg(next_temp_c) over (partition by device_code order by status_bits, id rows between 3 preceding and current row) as next_temp_c_avg_3
    from scored
    window w_recent as (partition by device_code order by next_temp_c nulls first)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        humidity,
        firmware,
        next_temp_c,
        next_temp_c_percent_rank,
        next_temp_c_avg_3
    from (
        select *, row_number() over (partition by device_code order by next_temp_c_avg_3 desc nulls first, id) as dedup_rank
        from enriched
    ) x
    where dedup_rank = 1
),
scored_device_readings as (
    -- Running and moving aggregates
    select
        *,
        lag(next_temp_c_percent_rank) over w_part as prev_next_temp_c_percent_rank,
        lead(next_temp_c_percent_rank) over w_part as next_next_temp_c_percent_rank,
        lag(humidity) over w_part as prev_humidity,
        row_number() over w_part as row_seq
    from latest
    window w_part as (order by next_temp_c_avg_3, id)
)
select
    id,
    humidity,
    firmware,
    next_temp_c_percent_rank,
    next_temp_c_avg_3,
    prev_next_temp_c_percent_rank,
    next_next_temp_c_percent_rank,
    prev_humidity,
    row_seq
from scored_device_readings
