WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        status_code,
        attr_key,
        ROUND(status_code, 0) AS status_code_rounded,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM flattened
    WHERE level IN ('WARN', 'ERROR')
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        host,
        attr_key,
        resolved_attr_key,
        CEIL(status_code) AS status_code_whole,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_clean
    FROM valid
    WHERE attr_key IS NOT DISTINCT FROM 'region'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_code_whole) OVER (PARTITION BY host, attr_key ORDER BY status_code_whole, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_status_code_whole,
        MIN(status_code_whole) OVER (PARTITION BY host, attr_key ORDER BY status_code_whole, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_status_code_whole,
        SUM(status_code_whole) OVER (PARTITION BY host, attr_key ORDER BY status_code_whole, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_status_code_whole
    FROM valid_app_logs
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        attr_key,
        resolved_attr_key,
        last7_avg_status_code_whole,
        last7_min_status_code_whole,
        last7_sum_status_code_whole,
        SUM(last7_min_status_code_whole) OVER w_grp AS last3_sum_last7_min_status_code_whole,
        AVG(last7_min_status_code_whole) OVER w_grp AS last3_avg_last7_min_status_code_whole,
        MAX(last7_min_status_code_whole) OVER w_grp AS last3_max_last7_min_status_code_whole
    FROM enriched
    WHERE attr_key != 'region'
    WINDOW w_grp AS (PARTITION BY attr_key ORDER BY last7_sum_status_code_whole, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY last3_max_last7_min_status_code_whole DESC NULLS LAST, id, attr_key) = 1
)
SELECT
    id,
    attr_key,
    resolved_attr_key,
    last7_avg_status_code_whole,
    last7_min_status_code_whole,
    last3_sum_last7_min_status_code_whole,
    last3_avg_last7_min_status_code_whole,
    last3_max_last7_min_status_code_whole
FROM ranked
ORDER BY last7_min_status_code_whole DESC NULLS FIRST;
