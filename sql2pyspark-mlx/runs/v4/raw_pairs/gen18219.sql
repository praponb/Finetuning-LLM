with cleaned as (
    select *, month(reading_ts) as reading_ts_month
    from device_readings
    where reading_ts > date '2025-09-01'
      and device_code not in ('DV-02', 'DV-04')
),
deduped as (
    select *
    from (
        select *, rank() over (partition by device_code order by humidity desc nulls first) as rank_in_group
        from cleaned
    ) ranked
    where rank_in_group = 1
),
with_windows as (
    select
        *,
        count(reading_ts_month) over (order by cast(reading_ts as bigint) range between 1800 preceding and current row) as reading_ts_month_count_30m,
        avg(reading_ts_month) over w_ordered as reading_ts_month_avg_2,
        round(pressure / sum(pressure) over (partition by device_code), 4) as pressure_share,
        avg(pressure) over (order by humidity, id rows between 6 preceding and current row) as pressure_avg_6
    from deduped
    window w_ordered as (partition by device_code order by status_bits, id rows between 2 preceding and current row)
),
kept_keys as (
    select firmware
    from with_windows
    except
    select firmware
    from device_readings
    where temp_c between 27 and 30
),
verified as (
    select s.*
    from with_windows s
    inner join kept_keys k on s.firmware = k.firmware
),
filtered as (
    select
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        reading_ts_month,
        rank_in_group,
        reading_ts_month_count_30m,
        reading_ts_month_avg_2,
        pressure_avg_6,
        case when device_code in ('DV-01', 'DV-04') then 'Core' else 'Other' end as device_code_group
    from verified
    where reading_ts > date '2025-09-01'
)
select
    case when humidity >= 68 then 'LARGE' else 'SMALL' end as humidity_tier,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as span_minutes,
    count(distinct id) as distinct_ids,
    min(reading_ts) as first_reading_ts
from filtered
group by case when humidity >= 68 then 'LARGE' else 'SMALL' end
order by humidity_tier desc nulls first;
