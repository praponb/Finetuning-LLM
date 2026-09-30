WITH valid AS (
    SELECT *, COALESCE(temp_c, 0) AS temp_c_filled
    FROM device_readings
    WHERE temp_c >= 31
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS rank_in_group
        FROM valid
    ) ranked
    WHERE rank_in_group = 1
),
latest AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        temp_c_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c_filled, id) AS rank_in_group_2
        FROM deduped
    ) t
    WHERE rank_in_group_2 = 1
),
valid_device_readings AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        temp_c_filled,
        CASE WHEN temp_c >= 38.2 THEN 'High' WHEN temp_c >= 18.2 THEN 'Medium' ELSE 'Low' END AS temp_c_band,
        temp_c / NULLIF(temp_c_filled, 0) AS temp_c_per_temp_c_filled
    FROM latest
    WHERE firmware IN ('2.0.1', '1.0.3', '1.1.0')
),
with_windows AS (
    SELECT
        *,
        SUM(temp_c_per_temp_c_filled) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS temp_c_per_temp_c_filled_sum_15m,
        COUNT(temp_c_per_temp_c_filled) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS temp_c_per_temp_c_filled_count_15m
    FROM valid_device_readings
),
metrics AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        temp_c_filled,
        temp_c_band,
        temp_c_per_temp_c_filled,
        temp_c_per_temp_c_filled_sum_15m,
        AVG(temp_c_per_temp_c_filled_count_15m) OVER (ORDER BY temp_c_filled, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS temp_c_per_temp_c_filled_count_15m_avg_6_prior,
        LEAD(temp_c_per_temp_c_filled_sum_15m, 2) OVER (ORDER BY temp_c_per_temp_c_filled_sum_15m, id) AS next_temp_c_per_temp_c_filled_sum_15m,
        AVG(temp_c_filled) OVER w_recent AS temp_c_filled_avg_5_prior
    FROM with_windows
    WHERE temp_c < 30
    WINDOW w_recent AS (ORDER BY temp_c_per_temp_c_filled_count_15m, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
)
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    PERCENTILE_APPROX(temp_c_per_temp_c_filled_count_15m_avg_6_prior, 0.95) AS temp_c_per_temp_c_filled_count_15m_avg_6_prior_p95,
    PERCENTILE_APPROX(temp_c_per_temp_c_filled_sum_15m, 0.95) AS temp_c_per_temp_c_filled_sum_15m_p95,
    APPROX_PERCENTILE(temp_c_per_temp_c_filled_sum_15m, ARRAY(0.25, 0.5, 0.75)) AS temp_c_per_temp_c_filled_sum_15m_quartiles
FROM metrics
GROUP BY DATE_TRUNC('DAY', reading_ts)
ORDER BY temp_c_per_temp_c_filled_count_15m_avg_6_prior_p95 DESC;
