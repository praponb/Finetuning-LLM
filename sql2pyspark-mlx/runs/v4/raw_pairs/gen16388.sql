WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        log_ts,
        level,
        status_code,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE status_code < 351
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(is_flagged) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS is_flagged_max_2h,
        PERCENT_RANK() OVER w_hist AS status_code_percent_rank,
        CUME_DIST() OVER (ORDER BY is_flagged ASC) AS is_flagged_cume_dist,
        AVG(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_high_avg_1h,
        CUME_DIST() OVER (ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_cume_dist
    FROM parsed
    WINDOW w_hist AS (PARTITION BY level ORDER BY status_code DESC)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(status_code AS STRING) AS status_code_str
    FROM metrics
    WHERE log_ts > DATE '2025-06-01'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_code_percent_rank) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_percent_rank_sum_2h,
        COUNT(is_flagged_max_2h) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS is_flagged_max_2h_count_15m,
        MAX(status_code_high) OVER (PARTITION BY level) AS level_max_status_code_high
    FROM base
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code >= 425 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    level,
    AVG(status_code_percent_rank) AS avg_status_code_percent_rank,
    MIN(log_ts) AS first_seen,
    BIT_OR(status_code) AS status_code_bit_or,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN status_code >= 425 THEN 'HIGH' ELSE 'LOW' END ORDER BY BIT_OR(status_code) DESC) AS status_code_bit_or_percent_rank
FROM enriched
GROUP BY CASE WHEN status_code >= 425 THEN 'HIGH' ELSE 'LOW' END, level;
