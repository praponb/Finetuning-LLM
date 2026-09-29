WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        status_bits,
        firmware,
        CAST(temp_c AS INT) AS temp_c_int
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(status_bits) OVER (ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_status_bits,
        AVG(status_bits) OVER (ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_status_bits,
        MIN(status_bits) OVER (ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_status_bits
    FROM valid
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY last7_max_status_bits DESC, id) AS row_seq,
        LAST_VALUE(last7_max_status_bits, TRUE) OVER (ORDER BY last7_min_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last7_max_status_bits,
        LAST_VALUE(last7_avg_status_bits, TRUE) OVER (ORDER BY last7_max_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last7_avg_status_bits
    FROM enriched
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY temp_c_int DESC NULLS LAST) AS temp_c_int_rank,
        NTILE(10) OVER (ORDER BY row_seq, id) AS row_seq_bucket
    FROM scored
    WHERE status_bits > 8
),
metrics_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(row_seq) OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        CUME_DIST() OVER (ORDER BY last_known_last7_max_status_bits DESC NULLS FIRST) AS last_known_last7_max_status_bits_cume_dist,
        SUM(status_bits) OVER (ORDER BY last_known_last7_avg_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits
    FROM metrics
)
SELECT
    id,
    status_bits,
    firmware,
    temp_c_int,
    last7_max_status_bits,
    last7_avg_status_bits,
    last7_min_status_bits,
    last_known_last7_avg_status_bits,
    temp_c_int_rank
FROM metrics_device_readings;
