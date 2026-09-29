WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(status_bits) OVER (PARTITION BY device_code ORDER BY reading_ts, id) next_status_bits,
        LAG(status_bits) OVER (PARTITION BY device_code ORDER BY reading_ts, id) prev_status_bits,
        LAG(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id) prev_pressure,
        LEAD(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id) next_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts, id) seq_no,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY pressure, id) pressure_bucket
    FROM device_readings
    WHERE firmware IN ('2.0.1', '1.0.3')
),
enriched_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(next_status_bits) OVER w_grp prev_next_status_bits,
        LEAD(next_status_bits) OVER w_grp next_next_status_bits,
        LEAD(humidity) OVER w_grp next_humidity,
        ROW_NUMBER() OVER w_grp event_seq,
        CORR(status_bits, humidity) OVER w_hist corr_status_bits_humidity
    FROM enriched
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY temp_c, id), w_hist AS (PARTITION BY device_code)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM enriched_device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts, id) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        next_status_bits,
        prev_status_bits,
        prev_pressure,
        next_pressure,
        seq_no,
        pressure_bucket,
        prev_next_status_bits,
        next_next_status_bits,
        next_humidity,
        corr_status_bits_humidity,
        is_new_session,
        session_no,
        LEAST(pressure, next_status_bits) least_pressure_next_status_bits,
        GREATEST(next_pressure, prev_pressure) greatest_next_pressure_prev_pressure
    FROM ranked
    WHERE is_new_session IS NOT NULL
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    pressure,
    status_bits,
    firmware,
    next_status_bits,
    prev_status_bits
FROM base
ORDER BY device_code DESC NULLS LAST;
