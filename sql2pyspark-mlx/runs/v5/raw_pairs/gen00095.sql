WITH flattened AS (
    SELECT
        id,
        service,
        host,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, v
),
parsed AS (
    SELECT
        id,
        status_code,
        v,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE status_code BETWEEN 291 AND 409
),
base AS (
    SELECT
        *,
        ROUND(status_code_high, 2) status_code_high_rounded,
        status_code_high / NULLIF(has_fault, 0) status_code_high_per_has_fault
    FROM parsed
    WHERE status_code_high < 2
),
with_windows AS (
    SELECT
        id,
        v,
        has_fault,
        status_code_high,
        status_code_high_rounded,
        status_code_high_per_has_fault,
        RANK() OVER w_grp status_code_high_rank,
        CUME_DIST() OVER w_recent status_code_high_cume_dist,
        CUME_DIST() OVER (ORDER BY status_code_high) status_code_high_cume_dist_2,
        CUME_DIST() OVER w_recent status_code_high_cume_dist_3
    FROM base
    WHERE status_code_high_per_has_fault >= 32
    WINDOW w_grp AS (ORDER BY status_code_high DESC), w_recent AS (ORDER BY status_code_high DESC NULLS FIRST)
)
SELECT
    CASE WHEN status_code_high_rounded >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_rounded_tier,
    CASE WHEN status_code_high_cume_dist_2 >= 0.9 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_cume_dist_2_tier,
    ROUND(AVG(status_code_high_rank), 2) AS avg_status_code_high_rank,
    APPROX_PERCENTILE(status_code_high_rank, 0.5) AS status_code_high_rank_p50,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    APPROX_PERCENTILE(has_fault, 0.95) AS has_fault_p95,
    ROUND(AVG(status_code_high), 2) AS avg_status_code_high
FROM with_windows
GROUP BY ROLLUP (CASE WHEN status_code_high_rounded >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN status_code_high_cume_dist_2 >= 0.9 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY status_code_high_cume_dist_2_tier DESC;
