WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, attr_value
),
kept_keys AS (
    SELECT id, host
    FROM flattened
    INTERSECT
    SELECT id, host
    FROM app_logs
    WHERE latency_ms != 1744
),
kept_rows AS (
    SELECT s.*
    FROM flattened s
    INNER JOIN kept_keys k ON s.id = k.id AND s.host = k.host
),
scored AS (
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
        MAX(status_code) OVER (PARTITION BY k, host) AS k_max_status_code,
        CORR(status_code, latency_ms) OVER (PARTITION BY host) AS corr_status_code_latency_ms,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share_2
    FROM kept_rows
),
scored_app_logs AS (
    SELECT
        *,
        AVG(k_max_status_code) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS k_max_status_code_avg_2h,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY status_code_share_2 DESC NULLS FIRST) AS status_code_share_2_dense_rank
    FROM scored
),
base AS (
    SELECT
        *,
        SUBSTRING(host, 1, 2) AS host_prefix,
        COALESCE(k, 'N/A') AS resolved_k
    FROM scored_app_logs
    WHERE status_code_share_2_dense_rank BETWEEN 5 AND 6
)
SELECT
    CASE WHEN k_max_status_code >= 467 THEN 'HIGH' ELSE 'LOW' END AS k_max_status_code_tier,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    SUM(CASE WHEN host = 'h2' THEN k_max_status_code ELSE 0 END) AS h2_k_max_status_code
FROM base
GROUP BY ROLLUP (CASE WHEN k_max_status_code >= 467 THEN 'HIGH' ELSE 'LOW' END)
HAVING span_minutes >= 0
ORDER BY h2_k_max_status_code DESC;
