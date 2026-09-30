WITH flattened AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
flattened_attrs AS (
    SELECT
        id,
        host,
        status_code,
        attr_key,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged
    FROM flattened
    WHERE status_code >= 369
),
metrics AS (
    SELECT
        id,
        status_code,
        v,
        AVG(is_flagged) OVER (PARTITION BY host) AS host_avg_is_flagged,
        DENSE_RANK() OVER (ORDER BY status_code) AS status_code_dense_rank
    FROM flattened_attrs
    WHERE is_flagged BETWEEN 0 AND 1
),
cleaned AS (
    SELECT
        *,
        CEIL(status_code_dense_rank) AS status_code_dense_rank_whole,
        CAST(status_code_dense_rank AS DOUBLE) AS status_code_dense_rank_num
    FROM metrics
    WHERE host_avg_is_flagged < 0
),
valid AS (
    SELECT
        v,
        host_avg_is_flagged,
        status_code_dense_rank_whole,
        CEIL(status_code_dense_rank_whole) AS status_code_dense_rank_whole_whole
    FROM cleaned
    WHERE status_code_dense_rank_num IS NULL
),
filtered AS (
    SELECT *, CASE WHEN host_avg_is_flagged >= 1 THEN 'High' WHEN host_avg_is_flagged >= 0 THEN 'Medium' ELSE 'Low' END AS host_avg_is_flagged_band
    FROM valid
    WHERE host_avg_is_flagged < 0
),
base AS (
    SELECT
        *,
        COALESCE(status_code_dense_rank_whole, 0) AS status_code_dense_rank_whole_filled,
        ROUND(status_code_dense_rank_whole * 0.85, 2) AS status_code_dense_rank_whole_scaled
    FROM filtered
    WHERE status_code_dense_rank_whole_whole < 37
)
SELECT
    status_code_dense_rank_whole,
    status_code_dense_rank_whole_whole,
    host_avg_is_flagged_band,
    status_code_dense_rank_whole_filled
FROM base
ORDER BY status_code_dense_rank_whole DESC NULLS LAST;
