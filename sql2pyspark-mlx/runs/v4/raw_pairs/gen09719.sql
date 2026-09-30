WITH filtered AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        temp_c * humidity AS temp_c_times_humidity
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
      AND humidity BETWEEN 44 AND 68
),
with_windows AS (
    SELECT
        *,
        AVG(pressure) OVER w_hist AS pressure_avg_5_prior,
        LAST_VALUE(humidity) IGNORE NULLS OVER w_grp AS last_known_humidity,
        ROW_NUMBER() OVER (ORDER BY temp_c, id) AS event_seq
    FROM filtered
    WINDOW w_hist AS (ORDER BY pressure, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_grp AS (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        SUM(temp_c) OVER w_hist AS running_temp_c,
        SUM(temp_c) OVER w_hist AS cumulative_temp_c
    FROM with_windows
    WINDOW w_hist AS (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_device_readings AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        temp_c_times_humidity,
        pressure_avg_5_prior,
        last_known_humidity,
        event_seq,
        running_temp_c,
        cumulative_temp_c,
        LAG(temp_c_times_humidity, 1) OVER (ORDER BY temp_c, id) AS prev_temp_c_times_humidity,
        AVG(running_temp_c) OVER (ORDER BY last_known_humidity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_temp_c_avg_3_prior
    FROM metrics
),
metrics_device_readings AS (
    SELECT
        *,
        AVG(last_known_humidity) OVER w_running AS last_known_humidity_avg_3_prior,
        AVG(cumulative_temp_c) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS cumulative_temp_c_avg_30m,
        LAST_VALUE(pressure_avg_5_prior) IGNORE NULLS OVER (ORDER BY prev_temp_c_times_humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure_avg_5_prior,
        FIRST_VALUE(cumulative_temp_c) IGNORE NULLS OVER (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_temp_c
    FROM with_windows_device_readings
    WINDOW w_running AS (ORDER BY temp_c, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
)
SELECT
    CASE WHEN temp_c_times_humidity >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_times_humidity_tier,
    CASE WHEN first_known_cumulative_temp_c >= 171 THEN 'LARGE' ELSE 'SMALL' END AS first_known_cumulative_temp_c_tier,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(humidity, 0.5) AS humidity_p50,
    AVG(cumulative_temp_c_avg_30m) AS avg_cumulative_temp_c_avg_30m,
    GROUPING(CASE WHEN temp_c_times_humidity >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM metrics_device_readings
GROUP BY ROLLUP (CASE WHEN temp_c_times_humidity >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN first_known_cumulative_temp_c >= 171 THEN 'LARGE' ELSE 'SMALL' END)
