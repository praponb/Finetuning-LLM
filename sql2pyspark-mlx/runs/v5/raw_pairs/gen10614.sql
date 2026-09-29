WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        key,
        v,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE service = 'billing'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code_high DESC NULLS FIRST, id, key) AS rank_in_group
        FROM unpacked
    ) t
    WHERE rank_in_group = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT service, id
    FROM deduped
    INTERSECT ALL
    SELECT service, id
    FROM app_logs
    WHERE HOUR(log_ts) BETWEEN 12 AND 20
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN matched_keys k ON s.service = k.service AND s.id = k.id
)
SELECT
    service,
    flag_bit8,
    status_code_high,
    rank_in_group
FROM kept_rows
ORDER BY rank_in_group, flag_bit8 DESC;
