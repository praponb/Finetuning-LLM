WITH exploded AS (
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, attr_value
),
metrics AS (
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY level, key), 4) AS status_code_share,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY latency_ms ASC, id ASC, key ASC) AS event_seq
    FROM exploded
),
with_windows AS (
    SELECT
        *,
        LEAD(latency_ms) OVER w_part AS next_latency_ms,
        LAG(latency_ms) OVER w_part AS prev_latency_ms,
        LAG(status_code_share) OVER w_part AS prev_status_code_share,
        ROW_NUMBER() OVER w_part AS event_seq_2
    FROM metrics
    WHERE latency_ms > 1403
    WINDOW w_part AS (PARTITION BY level ORDER BY status_code_share ASC, id ASC, key ASC)
),
scored AS (
    SELECT
        id,
        level,
        latency_ms,
        key,
        attr_value,
        event_seq,
        prev_latency_ms,
        prev_status_code_share,
        event_seq_2,
        MAX(latency_ms) OVER (PARTITION BY level, key ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_latency_ms,
        SUM(latency_ms) OVER (PARTITION BY level, key ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_latency_ms,
        MIN(latency_ms) OVER (PARTITION BY level, key ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_latency_ms,
        AVG(prev_latency_ms) OVER (PARTITION BY key ORDER BY event_seq ASC, id ASC, key ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prev_latency_ms_avg_3
    FROM with_windows
),
latest AS (
    SELECT
        id,
        level,
        latency_ms,
        key,
        attr_value,
        event_seq_2,
        running_min_latency_ms,
        prev_latency_ms_avg_3
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level, key ORDER BY latency_ms DESC, id ASC, key ASC) AS dedup_rank
        FROM scored
    ) ranked
    WHERE dedup_rank = 1
),
kept_keys AS (
    SELECT id, level
    FROM latest
    INTERSECT
    SELECT id, level
    FROM app_logs
    WHERE host = 'h1'
),
verified AS (
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.id = k.id AND s.level = k.level
)
SELECT
    id,
    level,
    latency_ms,
    key,
    event_seq_2,
    running_min_latency_ms,
    CASE WHEN key RLIKE '^(region|trace)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS key_zone
FROM verified
ORDER BY key ASC, running_min_latency_ms ASC
