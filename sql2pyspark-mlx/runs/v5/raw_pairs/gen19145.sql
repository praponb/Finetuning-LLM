WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
unpacked AS (
    SELECT
        id,
        status_code,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
with_windows AS (
    SELECT
        status_code,
        is_flagged,
        status_code_high,
        PERCENT_RANK() OVER (ORDER BY is_flagged NULLS LAST) AS is_flagged_percent_rank,
        DENSE_RANK() OVER w_recent AS status_code_high_dense_rank
    FROM unpacked
    WINDOW w_recent AS (ORDER BY status_code_high DESC NULLS FIRST)
),
filtered AS (
    SELECT
        status_code,
        is_flagged,
        status_code_high_dense_rank,
        GREATEST(status_code, is_flagged_percent_rank) AS greatest_status_code_is_flagged_percent_rank
    FROM with_windows
    WHERE status_code_high_dense_rank <> 5
),
cleaned AS (
    SELECT *, ROUND(is_flagged / status_code_high_dense_rank, 2) AS is_flagged_per_status_code_high_dense_rank
    FROM filtered
    WHERE status_code_high_dense_rank <= 8
)
SELECT
    CASE WHEN is_flagged >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_flagged_tier,
    CASE WHEN greatest_status_code_is_flagged_percent_rank >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS greatest_status_code_is_flagged_percent_rank_tier,
    ROUND(AVG(is_flagged), 2) AS avg_is_flagged,
    PERCENTILE_APPROX(is_flagged_per_status_code_high_dense_rank, 0.5) AS is_flagged_per_status_code_high_dense_rank_p50,
    MIN(is_flagged_per_status_code_high_dense_rank) AS min_is_flagged_per_status_code_high_dense_rank,
    ROUND(AVG(greatest_status_code_is_flagged_percent_rank), 2) AS avg_greatest_status_code_is_flagged_percent_rank,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(is_flagged_per_status_code_high_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS is_flagged_per_status_code_high_dense_rank_quartiles,
    GROUPING_ID() AS grouping_level
FROM cleaned
GROUP BY CUBE (CASE WHEN is_flagged >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN greatest_status_code_is_flagged_percent_rank >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING avg_is_flagged > 0;
