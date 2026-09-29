WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST, id) AS row_num
    FROM device_readings
    QUALIFY row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity, id) AS event_seq
    FROM deduped
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM metrics
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        seq_no,
        is_new_session,
        burst_id,
        DAY(reading_ts) AS reading_ts_day,
        GREATEST(humidity, seq_no) AS greatest_humidity_seq_no
    FROM sessionized
    WHERE reading_ts > TIMESTAMP '2025-09-01 14:00:00'
      AND burst_id = 7
),
base_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        temp_c,
        status_bits,
        is_new_session,
        burst_id,
        reading_ts_day,
        CASE WHEN reading_ts_day >= 86 THEN 'High' WHEN reading_ts_day >= 14 THEN 'Medium' ELSE 'Low' END AS reading_ts_day_tier
    FROM base
    WHERE status_bits < 8
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status_bits,
        burst_id,
        reading_ts_day_tier,
        COALESCE(temp_c, 0) AS temp_c_filled,
        ROUND(temp_c, 2) AS temp_c_rounded
    FROM base_device_readings
    WHERE reading_ts_day BETWEEN 42 AND 51
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(temp_c_rounded) OVER (ORDER BY temp_c_filled, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS temp_c_rounded_avg_3,
        FIRST_VALUE(temp_c_filled, TRUE) OVER (ORDER BY temp_c_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c_filled,
        DENSE_RANK() OVER (ORDER BY temp_c_rounded NULLS LAST) AS temp_c_rounded_dense_rank,
        ROW_NUMBER() OVER (ORDER BY burst_id DESC, id) AS seq_no,
        AVG(burst_id) OVER (ORDER BY burst_id, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS burst_id_avg_2
    FROM filtered
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_bits >= 11 THEN 'LARGE' ELSE 'SMALL' END AS status_bits_tier,
    CASE WHEN temp_c_filled >= 63 THEN 'HIGH' ELSE 'LOW' END AS temp_c_filled_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(burst_id_avg_2, ARRAY(0.25, 0.5, 0.75)) AS burst_id_avg_2_quartiles,
    BIT_AND(status_bits) AS status_bits_bit_and,
    APPROX_PERCENTILE(temp_c_filled, 0.95) AS temp_c_filled_p95
FROM scored
GROUP BY CASE WHEN status_bits >= 11 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN temp_c_filled >= 63 THEN 'HIGH' ELSE 'LOW' END
ORDER BY distinct_ids DESC NULLS FIRST;
