WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c NULLS LAST, id) AS latest_rank
    FROM device_readings
    QUALIFY latest_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) AS rank_in_group
        FROM deduped
    ) x
    WHERE rank_in_group = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        humidity,
        status_bits,
        firmware,
        rank_in_group,
        NTILE(3) OVER (PARTITION BY device_code ORDER BY temp_c, id) temp_c_bucket,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY pressure DESC NULLS FIRST) pressure_dense_rank,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) humidity_avg_1h
    FROM ranked
),
enriched_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        status_bits,
        firmware,
        rank_in_group,
        temp_c_bucket,
        pressure_dense_rank,
        humidity_avg_1h,
        RANK() OVER w_recent status_bits_rank,
        AVG(status_bits) OVER w_grp device_code_avg_status_bits,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY temp_c_bucket, id) temp_c_bucket_bucket,
        LEAD(status_bits, 2) OVER (PARTITION BY device_code ORDER BY status_bits, id) next_status_bits
    FROM enriched
    WHERE status_bits <= 7
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY status_bits), w_grp AS (PARTITION BY device_code)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_status_bits >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_status_bits_tier,
    CASE WHEN temp_c_bucket_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS temp_c_bucket_bucket_tier,
    CASE WHEN status_bits >= 10 THEN 'LARGE' ELSE 'SMALL' END AS status_bits_tier,
    ROUND(SUM(temp_c_bucket) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS temp_c_bucket_per_dv_01,
    SUM(CASE WHEN device_code = 'DV-02' THEN status_bits_rank ELSE 0 END) AS dv_02_status_bits_rank,
    GROUPING_ID() AS gid
FROM enriched_device_readings
GROUP BY GROUPING SETS ((CASE WHEN next_status_bits >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN temp_c_bucket_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN status_bits >= 10 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN next_status_bits >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN temp_c_bucket_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END), ())
HAVING temp_c_bucket_per_dv_01 >= 0
ORDER BY temp_c_bucket_bucket_tier DESC;
