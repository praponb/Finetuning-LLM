WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN firmware IN ('1.0.3', '2.0.1') THEN 'Priority' ELSE 'Other' END AS firmware_group
    FROM device_readings
    WHERE status_bits BETWEEN 9 AND 11
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        firmware_group,
        pressure - status_bits AS pressure_minus_status_bits
    FROM cleaned
    WHERE humidity BETWEEN 44 AND 47
),
cleaned_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware_group,
        pressure_minus_status_bits,
        CEIL(pressure) AS pressure_whole
    FROM valid
    WHERE reading_ts > DATE '2025-09-01'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(pressure) OVER (ORDER BY pressure_minus_status_bits ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_pressure,
        SUM(pressure) OVER (ORDER BY pressure_minus_status_bits ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_pressure,
        AVG(pressure) OVER (ORDER BY pressure_minus_status_bits ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_pressure,
        NTILE(10) OVER (ORDER BY humidity DESC, id ASC) AS humidity_bucket
    FROM cleaned_device_readings
    WHERE pressure_whole < 22
)
-- Grouped metrics
SELECT
    CASE WHEN pressure_whole >= 70 THEN 'LARGE' ELSE 'SMALL' END AS pressure_whole_tier,
    CASE WHEN last4_avg_pressure >= 1023 THEN 'PREMIUM' ELSE 'STANDARD' END AS last4_avg_pressure_tier,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(last4_avg_pressure, 0.95) AS last4_avg_pressure_p95,
    BIT_OR(status_bits) AS status_bits_bit_or,
    ROUND(AVG(humidity_bucket), 2) AS avg_humidity_bucket,
    PERCENTILE_APPROX(pressure_minus_status_bits, 0.5) AS pressure_minus_status_bits_p50,
    BIT_OR(status_bits) AS status_bits_bit_or_2,
    GROUPING_ID() AS grouping_id
FROM metrics
GROUP BY GROUPING SETS ((CASE WHEN pressure_whole >= 70 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last4_avg_pressure >= 1023 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN pressure_whole >= 70 THEN 'LARGE' ELSE 'SMALL' END), ());
