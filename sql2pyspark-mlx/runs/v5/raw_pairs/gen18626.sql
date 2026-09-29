WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, attr_value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        service,
        level,
        attr_key,
        FLOOR(status_code) status_code_whole
    FROM flattened
    WHERE attr_key IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT service, level
    FROM cleaned
    EXCEPT
    SELECT service, level
    FROM app_logs
    WHERE latency_ms IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.service = k.service AND s.level = k.level
),
kept_keys AS (
    -- Set difference on keys
    SELECT service
    FROM reconciled
    EXCEPT DISTINCT
    SELECT service
    FROM app_logs
    WHERE host = 'h2'
),
reconciled_app_logs AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN kept_keys k ON s.service = k.service
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(attr_key, 1, 3) attr_key_prefix
    FROM reconciled_app_logs
    WHERE attr_key IS NOT NULL
)
SELECT
    service,
    attr_key,
    status_code_whole,
    attr_key_prefix
FROM cleaned_app_logs
ORDER BY service, attr_key
