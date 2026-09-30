WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        level,
        latency_ms,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged
    FROM lines
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        latency_ms,
        k,
        status_code_high,
        CAST(status_code_high AS STRING) AS status_code_high_num,
        CONCAT_WS('/', k, level) AS k_level_key
    FROM flattened_attrs
    WHERE k IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        level,
        latency_ms,
        k,
        status_code_high,
        status_code_high_num,
        k_level_key,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY latency_ms DESC, id, k) AS row_seq,
        AVG(status_code_high) OVER (ORDER BY status_code_high, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_code_high_avg_6_prior
    FROM filtered
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_high DESC NULLS FIRST, id, k) AS dedup_rank
        FROM scored
    ) ranked
    WHERE dedup_rank = 1
),
scored_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        latency_ms,
        k,
        status_code_high,
        status_code_high_num,
        k_level_key,
        row_seq,
        status_code_high_avg_6_prior,
        dedup_rank,
        LEAD(status_code_high_avg_6_prior, 1) OVER (PARTITION BY level ORDER BY latency_ms, id, k) AS next_status_code_high_avg_6_prior,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY level, k), 4) AS row_seq_share
    FROM ranked
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        k,
        CASE WHEN next_status_code_high_avg_6_prior >= 3 THEN 'LARGE' ELSE 'SMALL' END AS next_status_code_high_avg_6_prior_tier,
        ROUND(SUM(status_code_high_avg_6_prior) / NULLIF(COUNT(CASE WHEN level = 'INFO' THEN 1 END), 0), 2) AS status_code_high_avg_6_prior_per_info,
        ROUND(SUM(status_code_high) / NULLIF(COUNT(CASE WHEN level = 'ERROR' THEN 1 END), 0), 2) AS status_code_high_per_error,
        SUM(next_status_code_high_avg_6_prior) AS total_next_status_code_high_avg_6_prior,
        MAX(next_status_code_high_avg_6_prior) FILTER (WHERE level = 'DEBUG') AS debug_max_next_status_code_high_avg_6_prior,
        ROUND(SUM(status_code_high_avg_6_prior) / NULLIF(COUNT(CASE WHEN level = 'INFO' THEN 1 END), 0), 2) AS status_code_high_avg_6_prior_per_info_2,
        SUM(row_seq_share) AS total_row_seq_share,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY ROUND(SUM(status_code_high_avg_6_prior) / NULLIF(COUNT(CASE WHEN level = 'INFO' THEN 1 END), 0), 2) DESC) AS status_code_high_avg_6_prior_per_info_percent_rank
    FROM scored_app_logs
    GROUP BY k, next_status_code_high_avg_6_prior_tier
)
SELECT
    k,
    next_status_code_high_avg_6_prior_tier,
    status_code_high_per_error,
    total_next_status_code_high_avg_6_prior,
    debug_max_next_status_code_high_avg_6_prior,
    status_code_high_avg_6_prior_per_info_2
FROM stats
ORDER BY next_status_code_high_avg_6_prior_tier DESC, status_code_high_avg_6_prior_per_info_2 DESC;
