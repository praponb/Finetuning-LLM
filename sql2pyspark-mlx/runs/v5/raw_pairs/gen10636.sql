WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        service,
        status_code,
        attr_key,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share_2,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key, service), 4) AS status_code_share_3
    FROM exploded
    WHERE attr_key NOT IN ('region', 'version')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        status_code,
        attr_key,
        status_code_share_2,
        status_code_share_3,
        LEAST(status_code_share_3, status_code_share_2) AS least_status_code_share_3_status_code_share_2
    FROM with_windows
    WHERE attr_key IS NOT NULL
      AND attr_key NOT IN ('version', 'region')
),
filtered_app_logs AS (
    -- Basic cleansing and defaults
    SELECT attr_key, CAST(status_code_share_3 AS INT) AS status_code_share_3_int
    FROM filtered
    WHERE attr_key IS NOT NULL
      AND least_status_code_share_3_status_code_share_2 IS NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(status_code_share_3_int AS STRING) AS status_code_share_3_int_num,
        CAST(status_code_share_3_int AS DOUBLE) AS status_code_share_3_int_num_2,
        COALESCE(attr_key, 'DIRECT') AS attr_key_clean
    FROM filtered_app_logs
    WHERE attr_key = 'version'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        attr_key,
        status_code_share_3_int,
        status_code_share_3_int_num,
        status_code_share_3_int_num_2,
        GREATEST(status_code_share_3_int_num_2, status_code_share_3_int) AS greatest_status_code_share_3_int_num_2_status_code_share_3_int,
        CEIL(status_code_share_3_int) AS status_code_share_3_int_whole
    FROM valid
    WHERE attr_key IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN greatest_status_code_share_3_int_num_2_status_code_share_3_int >= 72 THEN 'HIGH' ELSE 'LOW' END AS greatest_status_code_share_3_int_num_2_status_code_share_3_int_tier,
    COUNT(*) AS num_rows,
    ROUND(SUM(status_code_share_3_int_whole) / NULLIF(COUNT(CASE WHEN attr_key = 'region' THEN 1 END), 0), 2) AS status_code_share_3_int_whole_per_region,
    ROUND(SUM(greatest_status_code_share_3_int_num_2_status_code_share_3_int) / NULLIF(COUNT(CASE WHEN attr_key = 'trace' THEN 1 END), 0), 2) AS greatest_status_code_share_3_int_num_2_status_code_share_3_int_per_trace
FROM cleaned
GROUP BY CASE WHEN greatest_status_code_share_3_int_num_2_status_code_share_3_int >= 72 THEN 'HIGH' ELSE 'LOW' END
ORDER BY num_rows ASC
