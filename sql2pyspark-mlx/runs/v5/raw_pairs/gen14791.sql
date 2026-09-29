WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, TO_DATE(reading_ts) reading_ts_date
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 04:00:00'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts_date DESC, id) row_seq,
        AVG(pressure) OVER w_ordered pressure_avg_3,
        LAST_VALUE(pressure) IGNORE NULLS OVER w_grp last_known_pressure
    FROM valid
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        firmware,
        row_seq,
        pressure_avg_3,
        SUM(last_known_pressure) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_last_known_pressure,
        AVG(temp_c) OVER (ORDER BY reading_ts, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) temp_c_avg_6_prior,
        SUM(temp_c) OVER (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_temp_c
    FROM scored
    WHERE reading_ts < '2024-03-01'
),
enriched_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(running_last_known_pressure) OVER w_ordered running_sum_running_last_known_pressure,
        MAX(running_last_known_pressure) OVER w_ordered running_max_running_last_known_pressure,
        COUNT(running_last_known_pressure) OVER w_ordered running_count_running_last_known_pressure,
        AVG(running_last_known_pressure) OVER w_ordered running_avg_running_last_known_pressure,
        AVG(running_last_known_pressure) OVER (ORDER BY row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) running_last_known_pressure_avg_2
    FROM enriched
    WINDOW w_ordered AS (ORDER BY temp_c_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(running_sum_running_last_known_pressure) OVER w_recent running_avg_running_sum_running_last_known_pressure,
        COUNT(running_sum_running_last_known_pressure) OVER w_recent running_count_running_sum_running_last_known_pressure,
        MIN(running_sum_running_last_known_pressure) OVER w_recent running_min_running_sum_running_last_known_pressure
    FROM enriched_device_readings
    WINDOW w_recent AS (ORDER BY cumulative_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        pressure_avg_3,
        temp_c_avg_6_prior,
        cumulative_temp_c,
        running_sum_running_last_known_pressure,
        running_max_running_last_known_pressure,
        running_count_running_last_known_pressure,
        running_avg_running_last_known_pressure,
        running_avg_running_sum_running_last_known_pressure,
        running_min_running_sum_running_last_known_pressure,
        NTILE(4) OVER (ORDER BY row_seq DESC, id) row_seq_bucket,
        LEAD(running_max_running_last_known_pressure, 1, 0) OVER (ORDER BY running_max_running_last_known_pressure, id) next_running_max_running_last_known_pressure
    FROM scored_device_readings
)
-- Grouped metrics
SELECT
    CASE WHEN pressure_avg_3 >= 1032 THEN 'PREMIUM' ELSE 'STANDARD' END AS pressure_avg_3_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(next_running_max_running_last_known_pressure, ARRAY(0.25, 0.5, 0.75)) AS next_running_max_running_last_known_pressure_quartiles,
    APPROX_PERCENTILE(row_seq_bucket, ARRAY(0.25, 0.5, 0.75)) AS row_seq_bucket_quartiles
FROM metrics
GROUP BY 1
