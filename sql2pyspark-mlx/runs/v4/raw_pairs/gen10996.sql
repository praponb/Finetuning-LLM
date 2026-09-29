WITH with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY humidity DESC) AS humidity_cume_dist,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure,
        ROW_NUMBER() OVER w_running AS event_seq,
        MIN(humidity) OVER (PARTITION BY device_code) AS device_code_min_humidity
    FROM device_readings
    WINDOW w_running AS (PARTITION BY device_code ORDER BY temp_c, id)
)
SELECT
    CASE WHEN humidity_cume_dist >= 0.74 THEN 'HIGH' ELSE 'LOW' END AS humidity_cume_dist_tier,
    APPROX_PERCENTILE(cumulative_pressure, 0.5) AS cumulative_pressure_p50,
    MAX(pressure) AS max_pressure,
    MAX(reading_ts) AS last_seen
FROM with_windows
GROUP BY 1
HAVING cumulative_pressure_p50 < 0
ORDER BY max_pressure;
