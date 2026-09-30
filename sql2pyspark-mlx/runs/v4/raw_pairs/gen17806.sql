WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(status_code) OVER (PARTITION BY host, level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_max_30m,
        CUME_DIST() OVER (PARTITION BY k ORDER BY status_code DESC) AS status_code_cume_dist,
        ROW_NUMBER() OVER (ORDER BY log_ts, id, k) AS event_seq
    FROM flattened
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(status_code, 1) AS status_code_rounded
    FROM scored
    WHERE log_ts >= TO_DATE('2025-06-01')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        log_ts,
        status_code,
        k,
        attr_value,
        status_code_max_30m,
        status_code_cume_dist,
        event_seq,
        CASE WHEN host = 'h1' THEN 1 ELSE 0 END AS is_h1
    FROM filtered
    WHERE log_ts > TO_DATE('2025-06-01')
      AND k IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(k, TRUE) OVER (PARTITION BY host, k ORDER BY is_h1, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k,
        AVG(status_code_max_30m) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_max_30m_avg_15m,
        RANK() OVER (PARTITION BY host ORDER BY is_h1 DESC) AS is_h1_rank,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code DESC, id, k) AS event_seq_2
    FROM base
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN status_code >= 443 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
        CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
        (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
        COUNT(*) AS num_rows,
        SIZE(COLLECT_SET(k)) AS num_ks
    FROM with_windows
    GROUP BY CASE WHEN status_code >= 443 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    status_code_tier,
    event_seq_tier,
    num_rows,
    num_ks
FROM daily
ORDER BY num_rows NULLS FIRST;
