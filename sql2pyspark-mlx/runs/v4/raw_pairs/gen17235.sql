WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_count_30m,
        MAX(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_max_30m,
        COUNT(latency_ms) OVER (PARTITION BY attr_key) AS attr_key_count_latency_ms
    FROM lines
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value,
        status_code_count_30m,
        status_code_max_30m,
        attr_key_count_latency_ms,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY status_code_count_30m DESC NULLS FIRST) AS status_code_count_30m_dense_rank,
        RANK() OVER (PARTITION BY attr_key, host ORDER BY status_code DESC) AS status_code_rank
    FROM scored
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code >= 403 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    CASE WHEN status_code_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_rank_tier,
    COUNT(DISTINCT host) AS distinct_hosts,
    MIN(log_ts) AS started_at,
    MAX(log_ts) AS last_seen,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN status_code >= 403 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(DISTINCT host) DESC) AS distinct_hosts_dense_rank
FROM with_windows
GROUP BY status_code_tier, status_code_rank_tier
