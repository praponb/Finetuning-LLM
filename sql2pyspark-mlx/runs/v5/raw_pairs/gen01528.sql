WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
kept_keys AS (
    -- Set difference on keys
    SELECT service
    FROM flattened
    EXCEPT ALL
    SELECT service
    FROM app_logs
    WHERE host <> 'h2'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN kept_keys k ON s.service = k.service
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(service) AS service_upper
    FROM reconciled
    WHERE service = 'auth'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(level, 1, 2) AS level_prefix,
        COALESCE(k, 'DIRECT') AS k_clean
    FROM base
    WHERE k IN ('trace', 'region')
)
-- Grouped metrics
SELECT
    CASE WHEN status_code >= 405 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE k = 'trace') AS trace_rows,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN level = 'ERROR' THEN 1 END), 0), 2) AS status_code_per_error,
    BIT_AND(status_code) AS status_code_bit_and,
    STDDEV_SAMP(status_code) AS status_code_stddev
FROM filtered
GROUP BY CASE WHEN status_code >= 405 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) < 1000
ORDER BY status_code_tier DESC
