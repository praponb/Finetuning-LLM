WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        status_code,
        k,
        value,
        CAST(status_code AS STRING) AS status_code_num,
        COALESCE(latency_ms, 0) AS latency_ms_filled
    FROM flattened
    WHERE k IS NOT NULL
      AND host NOT IN ('h2', 'h3', 'h1')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service, k ORDER BY status_code, id, k) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        status_code,
        k,
        value,
        status_code_num,
        latency_ms_filled,
        dedup_rank,
        SUM(status_code) OVER (ORDER BY dedup_rank, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code,
        AVG(latency_ms_filled) OVER (PARTITION BY host ORDER BY status_code, id, k ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS latency_ms_filled_avg_3_prior,
        COUNT(latency_ms_filled) OVER (PARTITION BY service) AS service_count_latency_ms_filled
    FROM latest
)
-- Grouped metrics
SELECT
    service,
    COUNT(*) FILTER (WHERE host = 'h3') AS h3_rows,
    COUNT(CASE WHEN host = 'h2' THEN 1 END) AS h2_count,
    COUNT(*) FILTER (WHERE service = 'checkout') AS checkout_rows,
    ROUND(SUM(service_count_latency_ms_filled) / NULLIF(COUNT(CASE WHEN k = 'trace' THEN 1 END), 0), 2) AS service_count_latency_ms_filled_per_trace,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list
FROM metrics
GROUP BY service;
