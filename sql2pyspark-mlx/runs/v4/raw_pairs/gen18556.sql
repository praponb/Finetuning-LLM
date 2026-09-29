WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS seq_no,
        ROW_NUMBER() OVER w_hist_2 AS row_seq,
        FIRST_VALUE(k, TRUE) OVER (PARTITION BY level, host ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY log_ts DESC, id, k) AS event_seq
    FROM flattened
    WINDOW w_hist AS (PARTITION BY k, host ORDER BY log_ts DESC, id, k), w_hist_2 AS (PARTITION BY level ORDER BY log_ts DESC, id, k)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host, level ORDER BY row_seq DESC NULLS LAST, id, k) AS rn
    FROM enriched
    QUALIFY rn = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        log_ts,
        level,
        k,
        v,
        seq_no,
        row_seq,
        first_known_k,
        event_seq,
        rn,
        ROW_NUMBER() OVER (ORDER BY row_seq DESC, id, k) AS event_seq_2,
        LAST_VALUE(k, TRUE) OVER (ORDER BY seq_no, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k,
        SUM(event_seq) OVER w_grp AS k_sum_event_seq
    FROM deduped
    WINDOW w_grp AS (PARTITION BY k)
)
-- Aggregate with conditional metrics
SELECT
    host,
    level,
    MAX(k_sum_event_seq) FILTER (WHERE host = 'h2') AS h2_max_k_sum_event_seq,
    COUNT(*) AS cnt
FROM with_windows
GROUP BY host, level
ORDER BY level DESC;
