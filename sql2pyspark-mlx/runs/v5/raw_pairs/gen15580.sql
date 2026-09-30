WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        level,
        status_code,
        k,
        LEAD(status_code, 2) OVER w_running AS next_status_code,
        CORR(latency_ms, status_code) OVER w_ordered AS corr_latency_ms_status_code
    FROM exploded
    WINDOW w_running AS (PARTITION BY k ORDER BY status_code, id, k), w_ordered AS (PARTITION BY service)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(corr_latency_ms_status_code / status_code, 2) AS corr_latency_ms_status_code_per_status_code
    FROM metrics
    WHERE service != 'auth'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        level,
        status_code,
        k
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY corr_latency_ms_status_code_per_status_code, id, k) = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(level)) AS level_title,
        COALESCE(k, 'NONE') AS k_filled
    FROM latest
    WHERE k = 'region'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, level
    FROM cleaned
    INTERSECT
    SELECT id, level
    FROM app_logs
    WHERE service = 'search'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.id = k.id AND s.level = k.level
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        level,
        status_code,
        PERCENT_RANK() OVER (ORDER BY status_code DESC) AS status_code_percent_rank
    FROM reconciled
)
-- Multi-level aggregation
SELECT
    level,
    SUM(CASE WHEN level = 'INFO' THEN status_code_percent_rank ELSE 0 END) AS info_status_code_percent_rank,
    BIT_AND(status_code) AS status_code_bit_and,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    PERCENTILE_APPROX(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    APPROX_PERCENTILE(status_code_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS status_code_percent_rank_quartiles
FROM metrics_app_logs
GROUP BY level
HAVING BIT_AND(status_code) < 1000
ORDER BY status_code_bit_and DESC
