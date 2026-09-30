WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        firmware,
        humidity - pressure AS humidity_minus_pressure
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
      AND humidity <= 53
),
base AS (
    -- Keep recent, valid rows
    SELECT *, temp_c + humidity_minus_pressure AS temp_c_plus_humidity_minus_pressure
    FROM filtered
    WHERE temp_c < 27
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST, id) AS dedup_rank
        FROM base
    ) ranked
    WHERE dedup_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER w_hist AS pressure_rank,
        SUM(temp_c_plus_humidity_minus_pressure) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c_plus_humidity_minus_pressure
    FROM latest
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY pressure NULLS LAST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN temp_c >= 35.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_tier,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS cnt_2,
    COUNT(DISTINCT device_code) AS distinct_device_codes,
    COUNT(*) AS cnt_3
FROM scored
GROUP BY 1
HAVING cnt_3 < 1
ORDER BY cnt NULLS FIRST;
