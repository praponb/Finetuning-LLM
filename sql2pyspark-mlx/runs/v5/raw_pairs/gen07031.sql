WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        SUBSTRING(firmware, 1, 4) AS firmware_prefix
    FROM device_readings
    WHERE status_bits = 9
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(firmware, 1, 2) AS firmware_prefix_2
    FROM base
    WHERE status_bits >= 4
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        firmware,
        firmware_prefix,
        firmware_prefix_2,
        LAST_VALUE(humidity) IGNORE NULLS OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity,
        ROW_NUMBER() OVER (ORDER BY humidity, id) AS event_seq,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM cleaned
    WINDOW w_grp AS (ORDER BY temp_c DESC, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        temp_c,
        pressure,
        firmware,
        firmware_prefix,
        last_known_humidity,
        event_seq,
        NTILE(5) OVER (ORDER BY humidity DESC, id) AS humidity_bucket,
        ROW_NUMBER() OVER (ORDER BY last_known_humidity DESC, id) AS event_seq_2,
        NTILE(4) OVER (ORDER BY event_seq DESC, id) AS event_seq_bucket
    FROM with_windows
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        firmware,
        firmware_prefix,
        last_known_humidity,
        event_seq_2,
        event_seq_bucket,
        LEAD(event_seq_bucket, 1) OVER (ORDER BY event_seq_2, id) AS next_event_seq_bucket,
        SUM(event_seq) OVER w_grp AS running_event_seq
    FROM metrics
    WINDOW w_grp AS (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN event_seq_2 >= 7 THEN 'HIGH' ELSE 'LOW' END AS event_seq_2_tier,
        CASE WHEN last_known_humidity >= 83 THEN 'HIGH' ELSE 'LOW' END AS last_known_humidity_tier,
        COUNT(*) AS cnt,
        COUNT(*) AS cnt_2,
        ROUND(AVG(event_seq_2), 2) AS avg_event_seq_2,
        COUNT(DISTINCT firmware) AS distinct_firmwares,
        ROUND(AVG(event_seq_2), 2) AS avg_event_seq_2_2
    FROM metrics_device_readings
    GROUP BY CASE WHEN event_seq_2 >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN last_known_humidity >= 83 THEN 'HIGH' ELSE 'LOW' END
    HAVING COUNT(*) >= 1
)
SELECT
    event_seq_2_tier,
    last_known_humidity_tier,
    cnt,
    distinct_firmwares,
    avg_event_seq_2_2
FROM totals
ORDER BY distinct_firmwares;
