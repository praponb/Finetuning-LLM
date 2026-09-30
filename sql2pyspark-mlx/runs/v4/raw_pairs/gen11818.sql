with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        humidity * temp_c as humidity_times_temp_c,
        status_bits + humidity as status_bits_plus_humidity
    from device_readings
    where reading_ts >= timestamp '2025-09-01 05:00:00'
      and pressure between 998 and 1011
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by device_code order by temp_c desc, id) as rn
        from base
    ) ranked
    where rn = 1
),
enriched as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by device_code order by temp_c nulls last) as temp_c_rank,
        avg(humidity_times_temp_c) over (order by temp_c, id rows between 3 preceding and current row) as humidity_times_temp_c_avg_3
    from deduped
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        humidity_times_temp_c - temp_c as humidity_times_temp_c_minus_temp_c,
        round(humidity_times_temp_c_avg_3 * 100, 2) as adjusted_humidity_times_temp_c_avg_3
    from enriched
    where rn <> 1
),
daily as (
    -- Aggregate with conditional metrics
    select
        device_code,
        case when status_bits_plus_humidity >= 77 then 'PREMIUM' else 'STANDARD' end as status_bits_plus_humidity_tier,
        avg(adjusted_humidity_times_temp_c_avg_3) filter (where device_code = 'DV-03') as dv_03_avg_adjusted_humidity_times_temp_c_avg_3,
        count_if(device_code = 'DV-03') as dv_03_events,
        sum(case when device_code = 'DV-04' then humidity_times_temp_c_minus_temp_c else 0 end) as dv_04_humidity_times_temp_c_minus_temp_c
    from filtered
    group by device_code, case when status_bits_plus_humidity >= 77 then 'PREMIUM' else 'STANDARD' end
    having dv_04_humidity_times_temp_c_minus_temp_c > 0
)
select
    device_code,
    status_bits_plus_humidity_tier,
    dv_03_avg_adjusted_humidity_times_temp_c_avg_3,
    dv_03_events,
    dv_04_humidity_times_temp_c_minus_temp_c
from daily;
