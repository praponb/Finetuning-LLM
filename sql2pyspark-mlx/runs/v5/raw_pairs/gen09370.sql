WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
),
unpacked AS (
    SELECT
        id,
        service,
        level,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged
    FROM lines
    WHERE level = 'DEBUG'
),
scored AS (
    SELECT
        id,
        service,
        level,
        key,
        status_code_high,
        NTILE(3) OVER (PARTITION BY key ORDER BY status_code_high, id, key) AS status_code_high_bucket,
        RANK() OVER (PARTITION BY level ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_rank
    FROM unpacked
),
base AS (
    SELECT
        id,
        key,
        status_code_high,
        status_code_high_bucket,
        status_code_high_rank,
        GREATEST(status_code_high_rank, status_code_high_bucket) AS greatest_status_code_high_rank_status_code_high_bucket
    FROM scored
    WHERE level NOT IN ('ERROR', 'WARN')
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY greatest_status_code_high_rank_status_code_high_bucket DESC NULLS FIRST, id, key) AS rn
    FROM base
    QUALIFY rn = 1
)
SELECT
    id,
    key,
    status_code_high,
    status_code_high_rank,
    rn
FROM latest
ORDER BY id
