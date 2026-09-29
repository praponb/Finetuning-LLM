WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity NULLS LAST, id) AS row_num
    FROM device_readings
    QUALIFY row_num = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY row_num DESC, id) AS latest_rank
    FROM latest
    QUALIFY latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(status_bits) OVER (PARTITION BY device_code) AS device_code_max_status_bits,
        LAST_VALUE(temp_c, TRUE) OVER w_recent AS last_known_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY latest_rank, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity, id) AS seq_no
    FROM deduped
    WINDOW w_recent AS (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        firmware,
        device_code_max_status_bits,
        last_known_temp_c,
        event_seq,
        SUM(temp_c) OVER (PARTITION BY device_code) AS device_code_sum_temp_c,
        LAST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS status_bits_avg_2,
        SUM(last_known_temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS last_known_temp_c_sum_30m
    FROM metrics
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        temp_c,
        firmware,
        event_seq,
        last_known_humidity,
        SUM(device_code_max_status_bits) OVER w_running AS cumulative_device_code_max_status_bits,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM scored
    WHERE device_code = 'DV-04'
    WINDOW w_running AS (PARTITION BY device_code ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY device_code ORDER BY last_known_humidity, id)
)
-- Grouped metrics
SELECT
    CASE WHEN last_known_humidity >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_humidity_tier,
    APPROX_PERCENTILE(temp_c, ARRAY(0.25, 0.5, 0.75)) AS temp_c_quartiles,
    COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count
FROM enriched
GROUP BY CASE WHEN last_known_humidity >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END
