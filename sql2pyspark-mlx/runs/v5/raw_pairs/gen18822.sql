WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        session_no,
        LEAD(humidity, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS next_humidity,
        NTILE(4) OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS status_bits_bucket
    FROM sessions_device_readings
    WHERE pressure IS NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY session_no DESC, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        session_no,
        status_bits_bucket,
        SUBSTRING(device_code, 1, 4) AS device_code_prefix
    FROM latest
    WHERE reading_ts > DATE '2025-09-01'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(status_bits_bucket) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY status_bits_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_bits_bucket,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS seq_no
    FROM base
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('DAY', reading_ts) AS activity_day,
        CASE WHEN first_known_status_bits_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_status_bits_bucket_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) AS dv_01_count,
        AVG(first_known_status_bits_bucket) FILTER (WHERE device_code = 'DV-01') AS dv_01_avg_first_known_status_bits_bucket,
        ROUND(AVG(status_bits), 2) AS avg_status_bits,
        AVG(seq_no) AS avg_seq_no,
        SIZE(COLLECT_SET(device_code)) AS num_device_codes,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('DAY', reading_ts) ORDER BY COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) DESC) AS dv_01_count_dense_rank
    FROM metrics
    GROUP BY activity_day, first_known_status_bits_bucket_tier
    HAVING dv_01_count > 1
)
SELECT
    activity_day,
    first_known_status_bits_bucket_tier,
    distinct_ids,
    dv_01_count,
    dv_01_avg_first_known_status_bits_bucket,
    avg_status_bits,
    avg_seq_no,
    num_device_codes,
    dv_01_count_dense_rank
FROM stats
ORDER BY first_known_status_bits_bucket_tier DESC NULLS FIRST
