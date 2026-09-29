WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        status_code,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM lines
    WHERE status_code BETWEEN 323 AND 383
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status_code,
        attr_key,
        status_code_high,
        has_fault,
        FIRST_VALUE(status_code_high, TRUE) OVER w_grp AS first_known_status_code_high,
        ROW_NUMBER() OVER w_part AS event_seq,
        SUM(status_code_high) OVER w_running AS attr_key_sum_status_code_high,
        LAG(has_fault, 1) OVER w_part AS prev_has_fault,
        NTILE(3) OVER (PARTITION BY attr_key ORDER BY status_code ASC, id ASC, attr_key ASC) AS status_code_bucket
    FROM flattened_attrs
    WINDOW w_grp AS (PARTITION BY attr_key ORDER BY status_code ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY attr_key ORDER BY has_fault ASC, id ASC, attr_key ASC), w_running AS (PARTITION BY attr_key)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status_code,
        attr_key,
        has_fault,
        first_known_status_code_high,
        prev_has_fault,
        status_code_bucket,
        CAST(status_code_bucket AS STRING) AS status_code_bucket_num
    FROM metrics
    WHERE attr_key IS NOT NULL
)
-- Grouped metrics
SELECT
    attr_key,
    COUNT(*) FILTER (WHERE attr_key = 'region') AS region_rows,
    PERCENTILE_APPROX(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    MIN(status_code_bucket) AS min_status_code_bucket,
    SUM(CASE WHEN attr_key = 'region' THEN has_fault ELSE 0 END) AS region_has_fault,
    COUNT(DISTINCT id) AS distinct_ids,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys
FROM base
GROUP BY 1
HAVING COUNT(DISTINCT id) >= 1
ORDER BY num_attr_keys DESC;
