WITH lines AS (
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, v
),
flattened_attrs AS (
    SELECT
        id,
        level,
        key,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE key = 'trace'
),
ranked AS (
    SELECT *
    FROM flattened_attrs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code_high DESC, id, key) = 1
),
metrics AS (
    SELECT
        id,
        key,
        is_flagged,
        SUM(status_code_high) OVER w_ordered AS running_sum_status_code_high,
        AVG(status_code_high) OVER w_ordered AS running_avg_status_code_high,
        COUNT(status_code_high) OVER w_ordered AS running_count_status_code_high,
        NTILE(10) OVER (ORDER BY is_flagged, id, key) AS is_flagged_bucket
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY key ORDER BY is_flagged, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT
        id,
        key,
        is_flagged,
        running_avg_status_code_high,
        running_count_status_code_high,
        is_flagged_bucket,
        CEIL(is_flagged_bucket) AS is_flagged_bucket_whole,
        CASE WHEN is_flagged >= 1 THEN 'High' WHEN is_flagged >= 0 THEN 'Medium' ELSE 'Low' END AS is_flagged_tier
    FROM metrics
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'region'
)
SELECT
    id,
    key,
    is_flagged,
    is_flagged_bucket,
    DENSE_RANK() OVER (PARTITION BY key ORDER BY running_count_status_code_high NULLS FIRST) AS running_count_status_code_high_dense_rank,
    SUM(is_flagged) OVER (ORDER BY is_flagged_bucket, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_flagged
FROM cleaned;
