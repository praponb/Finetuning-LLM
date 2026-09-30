with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by device_code order by pressure nulls last, id) as row_num
        from device_readings
    ) ranked
    where row_num = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        reading_ts,
        temp_c,
        firmware,
        row_num
    from (
        select *, row_number() over (partition by device_code order by pressure, id) as latest_rank
        from deduped
    ) ranked
    where latest_rank = 1
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(temp_c) over (order by cast(reading_ts as bigint) range between 1800 preceding and current row) as temp_c_avg_30m,
        sum(temp_c) over (order by row_num, id rows between unbounded preceding and current row) as running_temp_c,
        ntile(5) over (order by temp_c, id) as temp_c_bucket,
        lag(temp_c, 2) over (order by reading_ts, id) as prev_temp_c
    from ranked
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(running_temp_c, 0) as running_temp_c_filled,
        ceil(temp_c) as temp_c_whole
    from metrics
    where reading_ts > date '2025-09-01'
),
metrics_device_readings as (
    -- Ranking and gap features
    select
        *,
        lead(running_temp_c_filled) over w_recent as next_running_temp_c_filled,
        lag(prev_temp_c) over w_recent as prev_prev_temp_c,
        lead(prev_temp_c) over w_recent as next_prev_temp_c,
        row_number() over w_recent as row_seq
    from cleaned
    window w_recent as (order by running_temp_c_filled, id)
),
enriched as (
    -- Ranking and gap features
    select
        *,
        sum(running_temp_c_filled) over (order by running_temp_c_filled, id rows between unbounded preceding and current row) as cumulative_running_temp_c_filled,
        cume_dist() over (order by prev_temp_c desc nulls first) as prev_temp_c_cume_dist
    from metrics_device_readings
    where next_running_temp_c_filled between 25 and 29
)
-- Window metrics over several different partitions
select
    *,
    lead(prev_temp_c_cume_dist) over (order by row_num, id) as next_prev_temp_c_cume_dist,
    lag(prev_temp_c_cume_dist) over (order by row_num, id) as prev_prev_temp_c_cume_dist,
    lag(row_seq) over (order by row_num, id) as prev_row_seq,
    row_number() over (order by row_num, id) as seq_no,
    avg(next_running_temp_c_filled) over (order by prev_temp_c_cume_dist, id rows between 3 preceding and 1 preceding) as next_running_temp_c_filled_avg_3_prior
from enriched;
