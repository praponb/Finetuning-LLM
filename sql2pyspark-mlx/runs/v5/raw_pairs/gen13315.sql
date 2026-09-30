WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        latency_ms,
        key,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, AVG(is_flagged) OVER w_part AS key_avg_is_flagged
    FROM unpacked
    WINDOW w_part AS (PARTITION BY key)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER w_hist AS key_avg_is_flagged_rank,
        MAX(is_flagged) OVER (PARTITION BY key) AS key_max_is_flagged
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY key ORDER BY key_avg_is_flagged DESC NULLS FIRST)
),
scored_app_logs AS (
    -- Running and moving aggregates
    SELECT
        key,
        key_avg_is_flagged,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY key_max_is_flagged DESC NULLS FIRST) AS key_max_is_flagged_percent_rank,
        STDDEV_SAMP(key_avg_is_flagged_rank) OVER w_ordered AS key_stddev_samp_key_avg_is_flagged_rank
    FROM scored
    WINDOW w_ordered AS (PARTITION BY key)
)
SELECT
    key,
    key_avg_is_flagged,
    key_max_is_flagged_percent_rank,
    key_stddev_samp_key_avg_is_flagged_rank
FROM scored_app_logs
ORDER BY key_stddev_samp_key_avg_is_flagged_rank DESC, key;
