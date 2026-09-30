WITH exploded AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
),
parsed AS (
    SELECT
        id,
        status_code,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM exploded
),
base AS (
    SELECT
        *,
        COALESCE(status_code_high, 0) status_code_high_filled,
        COALESCE(key, 'DIRECT') key_clean
    FROM parsed
    WHERE key IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code_high NULLS FIRST, id, key) AS rn
        FROM base
    ) t
    WHERE rn = 1
),
scored AS (
    SELECT
        id,
        status_code,
        key,
        status_code_high,
        has_fault,
        status_code_high_filled,
        key_clean,
        rn,
        AVG(status_code_high_filled) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) status_code_high_filled_avg_2_prior,
        LAG(status_code, 1, 0) OVER (PARTITION BY key ORDER BY has_fault, id, key) prev_status_code
    FROM latest
)
SELECT
    id,
    status_code,
    key,
    has_fault,
    status_code_high_filled,
    key_clean,
    rn,
    status_code_high_filled_avg_2_prior,
    prev_status_code,
    CASE WHEN key RLIKE '^(version|region)$' THEN 'CORE' ELSE 'GROUP_A' END AS key_zone
FROM scored
ORDER BY status_code_high_filled_avg_2_prior NULLS FIRST, status_code_high_filled;
