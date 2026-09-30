WITH latest AS (
    SELECT *
    FROM device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) = 1
),
base AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        COALESCE(humidity, 0) AS humidity_filled
    FROM latest
    WHERE device_code = 'DV-02'
),
cleaned AS (
    SELECT *, FLOOR(temp_c) AS temp_c_whole
    FROM base
    WHERE reading_ts > TIMESTAMP '2025-09-01 04:00:00'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c_whole NULLS FIRST, id) AS rn
        FROM cleaned
    ) t
    WHERE rn = 1
),
metrics AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        firmware,
        temp_c_whole,
        MAX(humidity) OVER w_hist AS running_max_humidity,
        AVG(humidity) OVER w_hist AS running_avg_humidity,
        SUM(humidity) OVER w_hist AS running_sum_humidity,
        LAST_VALUE(humidity) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity
    FROM ranked
    WHERE rn <= 1
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY temp_c_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_device_readings AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS FIRST, id) AS dedup_rank
        FROM metrics
    ) x
    WHERE dedup_rank = 1
)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        last_known_humidity,
        temp_c,
        running_max_humidity
    FROM latest_device_readings
) src
UNPIVOT (
    metric_value FOR metric IN (last_known_humidity AS LAST_KNOWN_HUMIDITY, temp_c AS TEMP_C, running_max_humidity AS RUNNING_MAX_HUMIDITY_VAL)
);
