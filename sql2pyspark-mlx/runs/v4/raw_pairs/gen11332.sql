WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        host,
        status_code,
        attr_key,
        value,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE host IN ('h4', 'h1')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        host,
        attr_key,
        value,
        status_code_high,
        CONCAT_WS('/', host, service) AS host_service_key
    FROM unpacked
    WHERE flag_bit2 > 1
)
-- Multi-level aggregation
SELECT
    attr_key,
    CASE WHEN status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_tier,
    COUNT(CASE WHEN attr_key = 'version' THEN 1 END) AS version_count,
    COUNT(DISTINCT host) AS distinct_hosts,
    ROUND(SUM(status_code_high) / NULLIF(COUNT(CASE WHEN attr_key = 'region' THEN 1 END), 0), 2) AS status_code_high_per_region,
    MAX(status_code_high) FILTER (WHERE host = 'h4') AS h4_max_status_code_high,
    RANK() OVER (PARTITION BY attr_key ORDER BY MAX(status_code_high) FILTER (WHERE host = 'h4') DESC) AS h4_max_status_code_high_rank
FROM valid
GROUP BY 1, 2;
