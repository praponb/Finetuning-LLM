with deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        reading_ts,
        pressure,
        firmware
    from (
        select *, row_number() over (partition by device_code order by humidity desc, id) as rank_in_group
        from device_readings
    ) ranked
    where rank_in_group = 1
),
scored as (
    -- Running and moving aggregates
    select
        *,
        count(pressure) over (order by cast(reading_ts as bigint) range between 1800 preceding and current row) as pressure_count_30m,
        avg(pressure) over (order by pressure, id rows between 3 preceding and 1 preceding) as pressure_avg_3_prior
    from deduped
    where firmware in ('2.0.1', '1.1.0')
),
scored_device_readings as (
    -- Window metrics over several different partitions
    select
        id,
        firmware,
        pressure_avg_3_prior,
        count(pressure_avg_3_prior) over (order by cast(reading_ts as bigint) range between 900 preceding and current row) as pressure_avg_3_prior_count_15m,
        avg(pressure_count_30m) over (order by pressure, id rows between 5 preceding and 1 preceding) as pressure_count_30m_avg_5_prior,
        row_number() over (order by pressure_avg_3_prior desc, id) as seq_no
    from scored
),
metrics as (
    -- Ranking and gap features
    select
        *,
        ntile(3) over (order by pressure_avg_3_prior, id) as pressure_avg_3_prior_bucket,
        first_value(pressure_avg_3_prior_count_15m, true) over (order by pressure_avg_3_prior, id rows between unbounded preceding and current row) as first_known_pressure_avg_3_prior_count_15m,
        row_number() over (order by pressure_avg_3_prior_count_15m, id) as seq_no_2,
        rank() over (order by pressure_avg_3_prior_count_15m desc nulls last) as pressure_avg_3_prior_count_15m_rank,
        ntile(3) over (order by seq_no desc, id) as seq_no_bucket
    from scored_device_readings
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        firmware,
        pressure_avg_3_prior,
        pressure_avg_3_prior_count_15m,
        seq_no,
        pressure_avg_3_prior_count_15m_rank,
        seq_no_bucket,
        case when firmware = '1.1.0' then 1 else 0 end as is_1_1_0
    from metrics
    where pressure_avg_3_prior_count_15m >= 9
)
select
    id,
    firmware,
    pressure_avg_3_prior,
    pressure_avg_3_prior_count_15m,
    seq_no,
    pressure_avg_3_prior_count_15m_rank,
    seq_no_bucket,
    is_1_1_0
from cleaned
order by firmware, id
