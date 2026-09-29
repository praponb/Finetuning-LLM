WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CASE WHEN firmware IN ('1.1.0', '1.0.3') THEN 'Tier 1' ELSE 'Other' END AS firmware_group
    FROM device_readings
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN pressure >= 1032 THEN 'High' WHEN pressure >= 989 THEN 'Medium' ELSE 'Low' END AS pressure_band,
        DATE_TRUNC('year', reading_ts) AS reading_ts_period
    FROM cleaned
    WHERE reading_ts > TIMESTAMP '2025-09-01 15:00:00'
      AND firmware = '2.0.1'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(pressure, 1) OVER (ORDER BY reading_ts ASC, id ASC) AS prev_pressure,
        ROW_NUMBER() OVER (ORDER BY humidity DESC, id ASC) AS event_seq,
        NTILE(10) OVER (ORDER BY temp_c ASC, id ASC) AS temp_c_bucket,
        DENSE_RANK() OVER (ORDER BY status_bits DESC) AS status_bits_dense_rank,
        DENSE_RANK() OVER (ORDER BY pressure ASC NULLS FIRST) AS pressure_dense_rank
    FROM valid
),
with_windows_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER (ORDER BY temp_c DESC, id ASC) AS temp_c_bucket_2,
        NTILE(5) OVER (ORDER BY humidity ASC, id ASC) AS humidity_bucket
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', reading_ts_period) AS year_start,
    DATE_TRUNC('QUARTER', reading_ts_period) AS quarter_start,
    CASE WHEN event_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    MIN(humidity) AS min_humidity,
    APPROX_PERCENTILE(temp_c_bucket_2, ARRAY(0.25, 0.5, 0.75)) AS temp_c_bucket_2_quartiles,
    STDDEV_SAMP(humidity) AS humidity_stddev,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    BIT_AND(status_bits) AS status_bits_bit_and
FROM with_windows_device_readings
GROUP BY DATE_TRUNC('YEAR', reading_ts_period), DATE_TRUNC('QUARTER', reading_ts_period), CASE WHEN event_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END;
