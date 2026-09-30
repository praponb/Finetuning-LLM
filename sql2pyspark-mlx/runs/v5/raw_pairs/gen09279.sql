WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN humidity >= 81 THEN 'High' WHEN humidity >= 40 THEN 'Medium' ELSE 'Low' END AS humidity_band
    FROM device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 01:00:00')
      AND pressure <= 1008
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        humidity_band,
        LEAD(status_bits, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS next_status_bits,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) AS row_seq
    FROM valid
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(humidity) OVER w_recent AS running_count_humidity,
        MIN(humidity) OVER w_recent AS running_min_humidity,
        MAX(humidity) OVER w_recent AS running_max_humidity,
        AVG(humidity) OVER w_recent AS running_avg_humidity,
        NTILE(4) OVER (ORDER BY humidity, id) AS humidity_bucket
    FROM metrics
    WINDOW w_recent AS (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        temp_c,
        pressure,
        status_bits,
        firmware,
        running_count_humidity,
        running_max_humidity,
        humidity_bucket,
        AVG(humidity) OVER w_part AS running_avg_humidity_2,
        COUNT(humidity) OVER w_part AS running_count_humidity_2,
        SUM(humidity) OVER w_part AS running_sum_humidity,
        SUM(status_bits) OVER (ORDER BY next_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits
    FROM scored
    WINDOW w_part AS (ORDER BY humidity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    pressure,
    status_bits,
    firmware,
    running_count_humidity,
    running_max_humidity,
    humidity_bucket,
    running_count_humidity_2
FROM enriched
ORDER BY firmware DESC;
