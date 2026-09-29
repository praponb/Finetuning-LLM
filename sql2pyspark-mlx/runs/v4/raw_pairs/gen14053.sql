with deduped as (
    select *
    from (
        select *, rank() over (partition by device_code order by temp_c desc) as dedup_rank
        from device_readings
    ) t
    where dedup_rank = 1
),
metrics as (
    select
        *,
        lag(temp_c, 1) over (order by status_bits, id) as prev_temp_c,
        lag(status_bits, 1) over (order by pressure, id) as prev_status_bits,
        min(humidity) over (partition by device_code) as device_code_min_humidity
    from deduped
),
matched_keys as (
    select device_code, id
    from metrics
    intersect all
    select device_code, id
    from device_readings
    where temp_c >= 28
),
kept_rows as (
    select s.*
    from metrics s
    inner join matched_keys k on s.device_code = k.device_code and s.id = k.id
),
remaining_keys as (
    select id, firmware
    from kept_rows
    except
    select id, firmware
    from device_readings
    where device_code not in ('DV-03', 'DV-01')
),
reconciled as (
    select s.*
    from kept_rows s
    inner join remaining_keys k on s.id = k.id and s.firmware = k.firmware
)
select
    case when temp_c >= 34.7 then 'HIGH' else 'LOW' end as temp_c_tier,
    case when humidity >= 67 then 'LARGE' else 'SMALL' end as humidity_tier,
    count(distinct firmware) as distinct_firmwares,
    bit_and(status_bits) as status_bits_bit_and,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as span_minutes
from reconciled
group by case when temp_c >= 34.7 then 'HIGH' else 'LOW' end, case when humidity >= 67 then 'LARGE' else 'SMALL' end
