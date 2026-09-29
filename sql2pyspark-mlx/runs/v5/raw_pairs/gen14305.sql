WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
parsed AS (
    -- Higher-order array functions
    SELECT
        level,
        status_code,
        key,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key, level), 4) AS status_code_share,
        MAX(status_code) OVER (PARTITION BY level, key) AS level_max_status_code,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key, level), 4) AS status_code_share_2
    FROM parsed
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, status_code_share - status_code_high AS status_code_share_minus_status_code_high
    FROM metrics
    WHERE key = 'region'
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_code_high) OVER (PARTITION BY level, key) AS level_avg_status_code_high,
        COUNT(status_code_high) OVER w_grp AS key_count_status_code_high,
        MIN(status_code_share_2) OVER (PARTITION BY level) AS level_min_status_code_share_2,
        SUM(status_code_share_2) OVER w_grp AS key_sum_status_code_share_2,
        PERCENT_RANK() OVER (ORDER BY status_code_share DESC NULLS LAST) AS status_code_share_percent_rank
    FROM valid
    WHERE status_code >= 357
    WINDOW w_grp AS (PARTITION BY key)
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_share >= 0.77 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_share_tier,
    BIT_AND(status_code) AS status_code_bit_and,
    ROUND(AVG(status_code_share_percent_rank), 2) AS avg_status_code_share_percent_rank,
    COUNT(DISTINCT key) AS distinct_keys,
    COUNT(*) FILTER (WHERE key = 'version') AS version_rows,
    COUNT(DISTINCT key) AS distinct_keys_2
FROM metrics_app_logs
GROUP BY CASE WHEN status_code_share >= 0.77 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY distinct_keys_2 DESC NULLS LAST
