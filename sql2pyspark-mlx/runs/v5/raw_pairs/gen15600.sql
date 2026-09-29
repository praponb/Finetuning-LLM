WITH flattened AS (
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
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        k,
        value,
        LENGTH(service) service_length,
        COALESCE(k, 'OTHER') resolved_k
    FROM flattened
    WHERE latency_ms < 1730
),
ranked AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY k, host ORDER BY service_length, id, k) = 1
),
ranked_app_logs AS (
    SELECT
        id,
        service,
        k,
        value,
        service_length,
        resolved_k
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY service_length, id, k) AS row_num
        FROM ranked
    ) t
    WHERE row_num = 1
),
filtered AS (
    SELECT *, CASE WHEN k IN ('trace', 'version') THEN 'Group A' ELSE 'Other' END k_group
    FROM ranked_app_logs
    WHERE k IS NOT NULL
      AND k IS DISTINCT FROM 'version'
),
filtered_app_logs AS (
    SELECT
        *,
        ROUND(service_length, 0) service_length_rounded,
        COALESCE(k, 'OTHER') k_filled
    FROM filtered
    WHERE k IS NOT NULL
)
SELECT
    id,
    service,
    k,
    service_length,
    resolved_k,
    k_group,
    service_length_rounded,
    k_filled
FROM filtered_app_logs
ORDER BY service_length_rounded
