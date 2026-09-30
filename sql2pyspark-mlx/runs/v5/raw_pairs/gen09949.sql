WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
enriched AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        v,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY level, service), 4) AS status_code_share,
        MAX(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_max_2h,
        SUM(status_code) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_sum_1h,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key), 4) AS status_code_share_2,
        MAX(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_max_30m
    FROM flattened
),
base AS (
    SELECT
        service,
        log_ts,
        status_code_share,
        status_code_max_2h,
        LOWER(key) AS key_lower
    FROM enriched
    WHERE log_ts > DATE '2025-06-01'
      AND key IS DISTINCT FROM 'trace'
),
matched_keys AS (
    SELECT service
    FROM base
    EXCEPT
    SELECT service
    FROM app_logs
    WHERE level IN ('ERROR', 'DEBUG')
),
verified AS (
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.service = k.service
)
SELECT
    CASE WHEN status_code_share >= 0.74 THEN 'LARGE' ELSE 'SMALL' END AS status_code_share_tier,
    CASE WHEN status_code_max_2h >= 382 THEN 'LARGE' ELSE 'SMALL' END AS status_code_max_2h_tier,
    ROUND(AVG(status_code_max_2h), 2) AS avg_status_code_max_2h,
    MAX(log_ts) AS last_seen,
    ROUND(AVG(status_code_share), 2) AS avg_status_code_share,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list_2
FROM verified
GROUP BY CASE WHEN status_code_share >= 0.74 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN status_code_max_2h >= 382 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_status_code_share;
