WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(temp_c * 1.07, 2) AS temp_c_scaled
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND status_bits BETWEEN 5 AND 9
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        temp_c,
        pressure,
        status_bits,
        firmware,
        dedup_rank,
        COUNT(status_bits) OVER (PARTITION BY device_code) AS device_code_count_status_bits,
        ROW_NUMBER() OVER (ORDER BY status_bits DESC, id) AS event_seq
    FROM latest
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status_bits,
        dedup_rank,
        event_seq,
        LAST_VALUE(device_code_count_status_bits) IGNORE NULLS OVER (ORDER BY device_code_count_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_device_code_count_status_bits,
        AVG(status_bits) OVER (ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_bits_avg_2_prior,
        CUME_DIST() OVER (ORDER BY pressure DESC) AS pressure_cume_dist,
        NTILE(10) OVER (ORDER BY event_seq, id) AS event_seq_bucket,
        SUM(status_bits) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_bits
    FROM with_windows
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS event_seq_bucket_tier,
    CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    AVG(event_seq) AS avg_event_seq,
    BIT_OR(status_bits) AS status_bits_bit_or
FROM metrics
GROUP BY CUBE (CASE WHEN event_seq_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END)
HAVING avg_event_seq > 0
ORDER BY avg_event_seq DESC
