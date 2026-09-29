WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, attr_value
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY k ORDER BY log_ts DESC) AS row_num
    FROM flattened
    QUALIFY row_num = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, FIRST_VALUE(k) IGNORE NULLS OVER (PARTITION BY k ORDER BY row_num, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k
    FROM deduped
),
deduped_app_logs AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY log_ts DESC, id, k) = 1
)
-- Ranking and gap features
SELECT
    id,
    k,
    attr_value,
    row_num,
    ROW_NUMBER() OVER (PARTITION BY k ORDER BY log_ts DESC, id, k) AS seq_no,
    FIRST_VALUE(k) IGNORE NULLS OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k_2
FROM deduped_app_logs
