WITH lines AS (
    SELECT
        id,
        service,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
),
unpacked AS (
    SELECT
        id,
        status_code,
        k,
        v,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
valid AS (
    SELECT
        *,
        status_code / NULLIF(flag_bit8, 0) AS status_code_per_flag_bit8,
        COALESCE(k, 'UNKNOWN') AS k_clean
    FROM unpacked
    WHERE k IS NOT NULL
),
kept_keys AS (
    SELECT id
    FROM valid
    EXCEPT
    SELECT id
    FROM app_logs
    WHERE TO_DATE(log_ts) >= '2025-10-05'
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    JOIN kept_keys k ON s.id = k.id
)
SELECT
    k,
    CASE WHEN status_code_per_flag_bit8 >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_per_flag_bit8_tier,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    COUNT(*) FILTER (WHERE k = 'region') AS region_rows,
    GROUPING_ID() AS grouping_level
FROM kept_rows
GROUP BY GROUPING SETS ((k, CASE WHEN status_code_per_flag_bit8 >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END), (k))
ORDER BY k
