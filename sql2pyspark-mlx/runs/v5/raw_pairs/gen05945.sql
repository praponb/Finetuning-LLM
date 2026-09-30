WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        service,
        status_code,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM lines
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(service) AS service_length
    FROM flattened_attrs
    WHERE status_code_high <> 2
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(service) AS clean_service
    FROM cleaned
    WHERE status_code_high = 1
)
-- Grouped metrics
SELECT
    CASE WHEN status_code >= 398 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    service,
    MIN(flag_bit2) AS min_flag_bit2,
    BIT_AND(status_code) AS status_code_bit_and,
    PERCENTILE_APPROX(flag_bit2, 0.5) AS flag_bit2_p50,
    COUNT(CASE WHEN service = 'search' THEN 1 END) AS search_count,
    AVG(status_code) AS avg_status_code,
    RANK() OVER (PARTITION BY CASE WHEN status_code >= 398 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(CASE WHEN service = 'search' THEN 1 END) DESC) AS search_count_rank
FROM filtered
GROUP BY CASE WHEN status_code >= 398 THEN 'HIGH' ELSE 'LOW' END, service
ORDER BY avg_status_code DESC NULLS FIRST
