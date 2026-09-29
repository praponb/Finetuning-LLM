WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        log_ts,
        status_code,
        value,
        CONCAT_WS('-', host, k) AS host_k_key
    FROM exploded
    WHERE log_ts > DATE '2025-06-01'
      AND k IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(status_code) AS status_code_whole
    FROM filtered
    WHERE status_code BETWEEN 366 AND 376
),
metrics AS (
    -- Ranking and gap features
    SELECT
        log_ts,
        value,
        host_k_key,
        CUME_DIST() OVER (ORDER BY status_code_whole NULLS LAST) AS status_code_whole_cume_dist,
        COUNT(status_code_whole) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_whole_count_30m,
        SUM(status_code_whole) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_whole_sum_1h
    FROM cleaned
    WHERE status_code_whole > 68
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code_whole_count_30m DESC NULLS FIRST) AS status_code_whole_count_30m_cume_dist,
        RANK() OVER w_part AS status_code_whole_count_30m_rank
    FROM metrics
    WINDOW w_part AS (ORDER BY status_code_whole_count_30m DESC)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(status_code_whole_count_30m_rank) AS status_code_whole_count_30m_rank_whole
    FROM enriched
    WHERE log_ts > DATE '2025-06-01'
),
cleaned_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        value,
        status_code_whole_cume_dist,
        status_code_whole_count_30m,
        status_code_whole_sum_1h,
        status_code_whole_count_30m_cume_dist,
        status_code_whole_count_30m_rank,
        status_code_whole_count_30m_rank_whole,
        DATE_FORMAT(log_ts, 'yyyy-MM') AS log_ts_label
    FROM valid
    WHERE status_code_whole_count_30m_cume_dist >= 0.24
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_whole_sum_1h >= 331 THEN 'LARGE' ELSE 'SMALL' END AS status_code_whole_sum_1h_tier,
    CASE WHEN status_code_whole_count_30m >= 17 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_whole_count_30m_tier,
    APPROX_PERCENTILE(status_code_whole_count_30m_rank_whole, 0.9) AS status_code_whole_count_30m_rank_whole_p90,
    COUNT(*) AS num_rows,
    MIN(status_code_whole_count_30m_rank) AS min_status_code_whole_count_30m_rank,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(status_code_whole_count_30m_rank_whole, 0.95) AS status_code_whole_count_30m_rank_whole_p95,
    APPROX_PERCENTILE(status_code_whole_count_30m_rank, 0.9) AS status_code_whole_count_30m_rank_p90,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN status_code_whole_sum_1h >= 331 THEN 'LARGE' ELSE 'SMALL' END ORDER BY APPROX_PERCENTILE(status_code_whole_count_30m_rank_whole, 0.9) DESC) AS status_code_whole_count_30m_rank_whole_p90_percent_rank
FROM cleaned_app_logs
GROUP BY status_code_whole_sum_1h_tier, status_code_whole_count_30m_tier
