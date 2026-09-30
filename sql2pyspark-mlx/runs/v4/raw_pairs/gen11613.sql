WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        latency_ms,
        status_code,
        key,
        attr_value,
        MIN(status_code) OVER w_grp AS key_min_status_code,
        LAST_VALUE(key, TRUE) OVER w_recent AS last_known_key,
        SUM(status_code) OVER (ORDER BY status_code ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code,
        NTILE(10) OVER (PARTITION BY key ORDER BY status_code ASC, id ASC, key ASC) AS status_code_bucket,
        MAX(status_code) OVER w_grp AS key_max_status_code
    FROM flattened
    WINDOW w_grp AS (PARTITION BY key), w_recent AS (ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY key_max_status_code DESC, id ASC, key ASC) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(key_max_status_code, 0) AS key_max_status_code_filled,
        COALESCE(key, 'NONE') AS key_filled
    FROM deduped
    WHERE key IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        status_code,
        key,
        attr_value,
        key_min_status_code,
        cumulative_status_code,
        status_code_bucket,
        key_max_status_code,
        key_max_status_code_filled,
        key_filled,
        LENGTH(key) AS key_length,
        COALESCE(key, 'NONE') AS key_clean
    FROM valid
    WHERE cumulative_status_code < 761
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(status_code AS DOUBLE) AS status_code_num,
        COALESCE(key, 'NONE') AS key_clean_2
    FROM filtered
    WHERE key IS NOT NULL
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN key_max_status_code >= 452 THEN 'HIGH' ELSE 'LOW' END AS key_max_status_code_tier,
        key,
        ROUND(SUM(cumulative_status_code) / NULLIF(COUNT(CASE WHEN key = 'trace' THEN 1 END), 0), 2) AS cumulative_status_code_per_trace,
        SUM(status_code_num) AS total_status_code_num,
        ROUND(SUM(cumulative_status_code) / NULLIF(COUNT(CASE WHEN key = 'version' THEN 1 END), 0), 2) AS cumulative_status_code_per_version,
        PERCENTILE_APPROX(status_code_bucket, 0.5) AS status_code_bucket_p50,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list,
        COUNT(*) FILTER (WHERE key = 'region') AS region_rows
    FROM cleaned
    GROUP BY CASE WHEN key_max_status_code >= 452 THEN 'HIGH' ELSE 'LOW' END, key
    HAVING status_code_bucket_p50 < 0
)
SELECT
    key_max_status_code_tier,
    key,
    cumulative_status_code_per_trace,
    total_status_code_num,
    cumulative_status_code_per_version,
    status_code_bucket_p50,
    region_rows
FROM daily;
