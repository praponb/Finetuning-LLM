WITH valid AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        humidity - pressure AS humidity_minus_pressure,
        HOUR(reading_ts) AS reading_ts_hour
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
enriched AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        humidity_minus_pressure,
        reading_ts_hour,
        ROW_NUMBER() OVER w_grp AS seq_no,
        ROW_NUMBER() OVER (ORDER BY humidity DESC, id) AS seq_no_2,
        AVG(humidity_minus_pressure) OVER w_running AS humidity_minus_pressure_avg_6,
        LAG(humidity, 1) OVER (ORDER BY temp_c, id) AS prev_humidity
    FROM valid
    WINDOW w_grp AS (ORDER BY reading_ts_hour DESC, id), w_running AS (ORDER BY reading_ts_hour, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), reading_ts) AS days_since_reading_ts
    FROM enriched
    WHERE humidity_minus_pressure > 49
)
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    seq_no,
    APPROX_PERCENTILE(prev_humidity, 0.5) AS prev_humidity_p50,
    MAX(reading_ts) AS last_seen,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM base
GROUP BY ROLLUP (DATE_TRUNC('DAY', reading_ts), seq_no)
