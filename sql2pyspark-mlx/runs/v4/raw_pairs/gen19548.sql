WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
        FIRST_VALUE(key, TRUE) OVER (PARTITION BY host ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_key
    FROM lines
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(latency_ms) IGNORE NULLS OVER (PARTITION BY service ORDER BY latency_ms_share ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms,
        NTILE(5) OVER (PARTITION BY key ORDER BY latency_ms DESC, id ASC, key ASC) AS latency_ms_bucket,
        AVG(latency_ms) OVER (PARTITION BY key, service ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS latency_ms_avg_6,
        AVG(latency_ms) OVER (PARTITION BY host ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_6_prior,
        NTILE(4) OVER (ORDER BY latency_ms ASC, id ASC, key ASC) AS latency_ms_bucket_2
    FROM metrics
)
SELECT
    id,
    service,
    host,
    latency_ms,
    key,
    value,
    latency_ms_share,
    first_known_key,
    latency_ms_bucket
FROM with_windows;
