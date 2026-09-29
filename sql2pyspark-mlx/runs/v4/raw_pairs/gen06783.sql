with ranked as (
    -- Keep one row per key (latest first)
    select *
    from device_readings
    qualify row_number() over (partition by device_code order by temp_c nulls first, id) = 1
),
filtered as (
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
        round(status_bits / temp_c, 2) status_bits_per_temp_c
    from ranked
    where reading_ts > date '2025-09-01'
      and firmware in ('1.0.3', '1.1.0')
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '15' minute then 1 else 0 end as is_new_burst
    from filtered
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            firmware,
            device_code,
            status_bits
        from islands
    ) src
    pivot (
        avg(status_bits)
        for device_code in ('DV-03' as dv_03, 'DV-04' as dv_04, 'DV-02' as dv_02)
    )
)
select firmware, dv_03
from by_type
order by dv_03 nulls first
