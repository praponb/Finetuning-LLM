WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', service, key) AS service_key_key,
        COALESCE(key, 'UNKNOWN') AS resolved_key
    FROM exploded
    WHERE key IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        value,
        service_key_key,
        resolved_key,
        CASE WHEN key = 'region' THEN 1 ELSE 0 END AS is_region
    FROM valid
    WHERE service = 'search'
),
kept_keys AS (
    -- Set difference on keys
    SELECT service, id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT service, id
    FROM app_logs
    WHERE TO_DATE(log_ts) < '2023-07-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.service = k.service AND s.id = k.id
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN is_region >= 74 THEN 'HIGH' ELSE 'LOW' END AS is_region_tier,
        service,
        APPROX_PERCENTILE(is_region, ARRAY(0.25, 0.5, 0.75)) AS is_region_quartiles,
        COUNT_IF(service = 'checkout') AS checkout_events,
        MAX(is_region) FILTER (WHERE service = 'auth') AS auth_max_is_region
    FROM verified
    GROUP BY CASE WHEN is_region >= 74 THEN 'HIGH' ELSE 'LOW' END, service
    HAVING MAX(is_region) FILTER (WHERE service = 'auth') >= 0
)
SELECT
    is_region_tier,
    service,
    is_region_quartiles,
    auth_max_is_region
FROM daily
ORDER BY is_region_tier DESC;
