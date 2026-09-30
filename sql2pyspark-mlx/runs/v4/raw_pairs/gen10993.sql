WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
),
base AS (
    -- Keep recent, valid rows
    SELECT
        service,
        log_ts,
        status_code,
        k,
        LENGTH(service) AS service_length
    FROM flattened
    WHERE k <> 'trace'
),
matched_keys AS (
    -- Set difference on keys
    SELECT service
    FROM base
    EXCEPT DISTINCT
    SELECT service
    FROM app_logs
    WHERE status_code <> 318
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.service = k.service
),
scored AS (
    -- Running and moving aggregates
    SELECT
        service,
        log_ts,
        status_code,
        k,
        service_length,
        SUM(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_sum_15m,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service, k), 4) AS status_code_share
    FROM reconciled
)
-- Grouped metrics
SELECT
    CASE WHEN status_code >= 450 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    MAX(log_ts) AS last_seen,
    ROUND(AVG(status_code_share), 2) AS avg_status_code_share,
    COUNT(CASE WHEN k = 'version' THEN 1 END) AS version_count,
    COUNT(CASE WHEN k = 'version' THEN 1 END) AS version_count_2,
    SUM(status_code) FILTER (WHERE service = 'checkout') AS checkout_total_status_code,
    COUNT(CASE WHEN service = 'billing' THEN 1 END) AS billing_count
FROM scored
GROUP BY status_code_tier
ORDER BY status_code_tier DESC NULLS LAST;
