with kept_keys as (
    -- Set difference on keys
    select firmware, id
    from device_readings
    except
    select firmware, id
    from device_readings
    where reading_ts >= '2024-02-29'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from device_readings s
    inner join kept_keys k on s.firmware = k.firmware and s.id = k.id
)
select
    id,
    device_code,
    reading_ts,
    humidity,
    pressure,
    status_bits,
    firmware
from reconciled;
