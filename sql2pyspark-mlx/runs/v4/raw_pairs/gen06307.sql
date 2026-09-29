with remaining_keys as (
    -- Set intersection on keys
    select device_code, id
    from device_readings
    intersect
    select device_code, id
    from device_readings
    where status_bits <> 6
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from device_readings s
    join remaining_keys k on s.device_code = k.device_code and s.id = k.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        corr(temp_c, humidity) over w_grp as corr_temp_c_humidity,
        avg(temp_c) over w_grp_2 as temp_c_avg_15m,
        count(pressure) over w_grp as device_code_count_pressure,
        round(pressure / sum(pressure) over (partition by device_code), 4) as pressure_share
    from reconciled
    window w_grp as (partition by device_code), w_grp_2 as (partition by device_code order by cast(reading_ts as bigint) range between 900 preceding and current row)
),
enriched as (
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
        corr_temp_c_humidity,
        temp_c_avg_15m,
        device_code_count_pressure,
        pressure_share,
        cume_dist() over (partition by device_code order by pressure desc nulls first) as pressure_cume_dist,
        avg(temp_c) over (order by cast(reading_ts as bigint) range between 900 preceding and current row) as temp_c_avg_15m_2,
        dense_rank() over (order by device_code_count_pressure desc nulls first) as device_code_count_pressure_dense_rank,
        rank() over (partition by device_code order by device_code_count_pressure) as device_code_count_pressure_rank
    from scored
),
valid as (
    -- Keep recent, valid rows
    select *, case when firmware = '2.0.1' then 1 else 0 end as is_2_0_1
    from enriched
    where reading_ts > to_date('2025-09-01')
      and temp_c_avg_15m between 33 and 39
),
valid_device_readings as (
    -- Filter the raw rows and derive helper columns
    select
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        corr_temp_c_humidity,
        temp_c_avg_15m,
        device_code_count_pressure,
        pressure_share,
        pressure_cume_dist,
        temp_c_avg_15m_2,
        device_code_count_pressure_dense_rank,
        device_code_count_pressure_rank,
        is_2_0_1,
        pressure * device_code_count_pressure_dense_rank as pressure_times_device_code_count_pressure_dense_rank
    from valid
    where is_2_0_1 = 74
)
-- Grouped metrics
select
    case when temp_c_avg_15m >= 35 then 'HIGH' else 'LOW' end as temp_c_avg_15m_tier,
    round(avg(is_2_0_1), 2) as avg_is_2_0_1,
    approx_percentile(temp_c_avg_15m_2, array(0.25, 0.5, 0.75)) as temp_c_avg_15m_2_quartiles,
    count(*) as event_count
from valid_device_readings
group by temp_c_avg_15m_tier
order by avg_is_2_0_1 desc;
