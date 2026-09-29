WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        log_ts,
        level,
        context,
        COALESCE(context['version'], 'none') AS version_or_default,
        context['region'] AS region,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        SIZE(context) AS num_context,
        context['version'] AS version
    FROM app_logs
)
-- Running and moving aggregates
SELECT
    *,
    SUM(num_context) OVER (PARTITION BY level ORDER BY num_context, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_num_context,
    DENSE_RANK() OVER (PARTITION BY level ORDER BY num_context NULLS FIRST) AS num_context_dense_rank,
    SUM(num_context) OVER (ORDER BY log_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_num_context
FROM unpacked
