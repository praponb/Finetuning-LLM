with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        humidity + status_bits as humidity_plus_status_bits
    from device_readings
    where status_bits > 9
),
enriched as (
    -- Ranking and gap features
    select
        *,
        lead(humidity) over w_ordered as next_humidity,
        lag(humidity) over w_ordered as prev_humidity,
        lead(pressure) over w_ordered as next_pressure,
        row_number() over w_ordered as row_seq
    from cleaned
    window w_ordered as (partition by device_code order by humidity, id)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        lag(next_pressure, 2, 0) over (partition by device_code order by pressure, id) as prev_next_pressure,
        avg(row_seq) over (partition by device_code order by cast(reading_ts as bigint) range between 1800 preceding and current row) as row_seq_avg_30m,
        row_number() over (partition by device_code order by status_bits, id) as seq_no,
        row_number() over (partition by device_code order by pressure desc, id) as event_seq,
        row_number() over (partition by device_code order by humidity_plus_status_bits, id) as seq_no_2
    from enriched
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        device_code,
        humidity,
        status_bits,
        humidity_plus_status_bits,
        next_humidity,
        prev_humidity,
        row_seq
    from (
        select *, row_number() over (partition by device_code order by row_seq_avg_30m desc nulls last, id) as latest_rank
        from metrics
    ) ranked
    where latest_rank = 1
),
remaining_keys as (
    -- Set difference on keys
    select id, device_code
    from latest
    except
    select id, device_code
    from device_readings
    where firmware = '1.1.0'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    inner join remaining_keys k on s.id = k.id and s.device_code = k.device_code
),
enriched_device_readings as (
    -- Ranking and gap features
    select *, avg(humidity) over (partition by device_code) as device_code_avg_humidity
    from kept_rows
),
enriched_device_readings_2 as (
    -- Ranking and gap features
    select
        id,
        device_code,
        row_seq,
        round(next_humidity / sum(next_humidity) over (partition by device_code), 4) as next_humidity_share,
        percent_rank() over w_hist as next_humidity_percent_rank
    from enriched_device_readings
    where row_seq < 7
    window w_hist as (partition by device_code order by next_humidity desc nulls first)
),
grouped as (
    -- Aggregate with conditional metrics
    select
        device_code,
        count(*) filter (where device_code = 'DV-02') as dv_02_rows,
        percentile_approx(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles
    from enriched_device_readings_2
    group by device_code
)
select
    device_code,
    dv_02_rows,
    row_seq_quartiles
from grouped
order by device_code;
