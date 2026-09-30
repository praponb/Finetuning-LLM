WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        level,
        latency_ms,
        key,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8
    FROM exploded
),
kept_keys AS (
    -- Set intersection on keys
    SELECT level
    FROM parsed
    INTERSECT ALL
    SELECT level
    FROM app_logs
    WHERE latency_ms BETWEEN 1545 AND 2083
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM parsed s
    INNER JOIN kept_keys k ON s.level = k.level
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        level,
        value,
        flag_bit8,
        PERCENT_RANK() OVER w_recent AS flag_bit8_percent_rank,
        RANK() OVER (PARTITION BY key ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_rank,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY status_code_high DESC) AS status_code_high_dense_rank
    FROM verified
    WHERE flag_bit8 = 1
    WINDOW w_recent AS (PARTITION BY key, level ORDER BY flag_bit8 DESC NULLS LAST)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT level
    FROM with_windows
    INTERSECT
    SELECT level
    FROM app_logs
    WHERE status_code BETWEEN 308 AND 442
),
verified_app_logs AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.level = k.level
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY flag_bit8 DESC NULLS FIRST) AS flag_bit8_dense_rank,
        ROUND(latency_ms_rank / SUM(latency_ms_rank) OVER (PARTITION BY level), 4) AS latency_ms_rank_share
    FROM verified_app_logs
),
scored_app_logs AS (
    -- Ranking and gap features
    SELECT
        value,
        flag_bit8_percent_rank,
        status_code_high_dense_rank,
        flag_bit8_dense_rank,
        latency_ms_rank_share,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY latency_ms_rank DESC NULLS FIRST) AS latency_ms_rank_dense_rank,
        RANK() OVER (PARTITION BY level ORDER BY latency_ms_rank DESC NULLS FIRST) AS latency_ms_rank_rank
    FROM scored
)
SELECT
    status_code_high_dense_rank,
    latency_ms_rank_share,
    latency_ms_rank_dense_rank
FROM scored_app_logs
ORDER BY latency_ms_rank_share DESC;
