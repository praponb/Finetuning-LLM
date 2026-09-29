WITH base AS (
    SELECT
        id,
        device_code,
        pressure,
        status_bits,
        firmware,
        LENGTH(firmware) AS firmware_length
    FROM device_readings
    WHERE status_bits >= 7
),
valid AS (
    SELECT
        id,
        pressure,
        firmware,
        firmware_length,
        LENGTH(firmware) AS firmware_length_2
    FROM base
    WHERE firmware IN ('1.1.0', '1.0.3')
),
scored AS (
    SELECT
        *,
        COUNT(firmware_length_2) OVER w_recent AS last7_count_firmware_length_2,
        MIN(firmware_length_2) OVER w_recent AS last7_min_firmware_length_2,
        MAX(firmware_length_2) OVER w_recent AS last7_max_firmware_length_2,
        AVG(firmware_length_2) OVER w_recent AS last7_avg_firmware_length_2
    FROM valid
    WINDOW w_recent AS (ORDER BY firmware_length_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        id,
        firmware,
        firmware_length,
        FIRST_VALUE(firmware_length_2, TRUE) OVER (ORDER BY firmware_length_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_firmware_length_2,
        ROW_NUMBER() OVER (ORDER BY last7_count_firmware_length_2, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY last7_avg_firmware_length_2 DESC, id) AS event_seq_2,
        AVG(firmware_length) OVER (ORDER BY last7_avg_firmware_length_2, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS firmware_length_avg_2,
        SUM(last7_avg_firmware_length_2) OVER (ORDER BY last7_min_firmware_length_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last7_avg_firmware_length_2
    FROM scored
)
SELECT
    CASE WHEN event_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    COUNT(*) AS row_count,
    AVG(first_known_firmware_length_2) AS avg_first_known_firmware_length_2,
    SUM(firmware_length) AS total_firmware_length
FROM enriched
GROUP BY CASE WHEN event_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END;
