WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM lines
    INTERSECT ALL
    SELECT id
    FROM app_logs
    WHERE log_ts >= '2024-11-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(latency_ms) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_max_2h,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY latency_ms NULLS LAST) AS latency_ms_dense_rank
    FROM verified
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        latency_ms,
        key,
        latency_ms_max_2h,
        HOUR(log_ts) AS log_ts_hour
    FROM enriched
    WHERE log_ts > TO_DATE('2025-06-01')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY key ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_rank,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank,
        SUM(latency_ms) OVER (PARTITION BY key) AS key_sum_latency_ms
    FROM cleaned
)
-- Aggregate with conditional metrics
SELECT
    key,
    MIN(latency_ms_rank) AS min_latency_ms_rank,
    ROUND(AVG(latency_ms_dense_rank), 2) AS avg_latency_ms_dense_rank
FROM scored
GROUP BY key
ORDER BY avg_latency_ms_dense_rank DESC NULLS LAST;
