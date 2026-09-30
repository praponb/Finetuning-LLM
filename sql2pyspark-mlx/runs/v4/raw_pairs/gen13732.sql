WITH flattened AS (
    SELECT
        id,
        service,
        host,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, v
),
valid AS (
    SELECT
        *,
        CONCAT_WS('-', k, host) AS k_host_key,
        CASE WHEN k IN ('region', 'trace') THEN 'Core' ELSE 'Other' END AS k_group
    FROM flattened
    WHERE host <> 'h4'
),
cleaned AS (
    SELECT
        id,
        service,
        host,
        k,
        v,
        k_host_key,
        k_group,
        LOWER(k) AS k_lower
    FROM valid
    WHERE k IS NOT NULL
),
totals AS (
    SELECT
        service,
        host,
        COUNT_IF(service = 'checkout') AS checkout_events,
        COUNT(*) AS event_count,
        GROUPING(service) AS grouping_id
    FROM cleaned
    GROUP BY ROLLUP (service, host)
    HAVING event_count < 1
)
SELECT
    service,
    host,
    checkout_events,
    event_count,
    grouping_id
FROM totals
