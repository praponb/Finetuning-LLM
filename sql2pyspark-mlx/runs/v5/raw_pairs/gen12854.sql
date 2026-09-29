WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        humidity * temp_c AS humidity_times_temp_c
    FROM device_readings
    WHERE humidity BETWEEN 41 AND 45
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY humidity_times_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c,
        LEAD(pressure, 1) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS next_pressure,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share
    FROM sessionized
),
enriched_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY first_known_temp_c DESC, id) AS seq_no,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_humidity,
        DENSE_RANK() OVER (ORDER BY humidity DESC NULLS FIRST) AS humidity_dense_rank,
        SUM(temp_c) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c,
        CUME_DIST() OVER (ORDER BY temp_c DESC NULLS LAST) AS temp_c_cume_dist
    FROM enriched
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched_device_readings
    INTERSECT ALL
    SELECT id
    FROM device_readings
    WHERE humidity <= 52
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_device_readings s
    INNER JOIN remaining_keys k ON s.id = k.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        device_code,
        humidity,
        next_pressure,
        first_known_temp_c
    FROM reconciled
) src
UNPIVOT (
    metric_value FOR measure IN (humidity, next_pressure, first_known_temp_c)
);
