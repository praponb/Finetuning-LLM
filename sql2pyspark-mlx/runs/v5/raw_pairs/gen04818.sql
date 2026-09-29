WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        level,
        k,
        v,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('/', k, level) AS k_level_key
    FROM flattened_attrs
    WHERE status_code_high = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code_high NULLS FIRST, id, k) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        level,
        k,
        status_code_high
    FROM deduped
) src
PIVOT (
    SUM(status_code_high)
    FOR k IN ('trace', 'version')
);
