WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        UPPER(device_code) AS device_code_upper
    FROM device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 07:00:00')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(humidity) IGNORE NULLS OVER (ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_humidity,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST) AS humidity_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id ASC) AS seq_no
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(first_known_humidity) OVER w_running AS next_first_known_humidity,
        LEAD(humidity) OVER w_running AS next_humidity,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM metrics
    WINDOW w_running AS (PARTITION BY device_code ORDER BY humidity ASC, id ASC)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS seq_no_2,
        CORR(humidity, pressure) OVER (PARTITION BY device_code) AS corr_humidity_pressure,
        NTILE(10) OVER w_part AS pressure_bucket
    FROM enriched
    WINDOW w_running AS (ORDER BY first_known_humidity DESC, id ASC), w_part AS (PARTITION BY device_code ORDER BY pressure DESC, id ASC)
)
SELECT
    id,
    device_code,
    reading_ts,
    pressure,
    firmware,
    device_code_upper,
    first_known_humidity,
    humidity_dense_rank,
    seq_no,
    CASE WHEN device_code RLIKE '^(DV-01|DV-02)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS device_code_zone
FROM with_windows
ORDER BY pressure DESC NULLS FIRST
