WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1
    FROM lines
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(attr_key) AS clean_attr_key
    FROM flattened_attrs
    WHERE flag_bit1 IS NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(flag_bit1) OVER w_running AS prev_flag_bit1,
        LEAD(status_code_high) OVER w_running AS next_status_code_high,
        LAG(status_code_high) OVER w_running AS prev_status_code_high,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM filtered
    WHERE flag_bit1 > 0
    WINDOW w_running AS (PARTITION BY attr_key ORDER BY flag_bit1, id, attr_key)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        attr_key,
        flag_bit1,
        clean_attr_key,
        prev_flag_bit1,
        prev_status_code_high,
        event_seq,
        SUM(prev_status_code_high) OVER w_hist AS cumulative_prev_status_code_high,
        NTILE(5) OVER w_recent AS flag_bit1_bucket
    FROM enriched
    WINDOW w_hist AS (PARTITION BY attr_key ORDER BY prev_flag_bit1, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY attr_key ORDER BY flag_bit1 DESC, id, attr_key)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY attr_key ORDER BY flag_bit1_bucket) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY attr_key ORDER BY prev_flag_bit1 NULLS LAST) AS dedup_rank_2
    FROM deduped
    QUALIFY dedup_rank_2 = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_prev_status_code_high >= 11 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_prev_status_code_high_tier,
    MAX(prev_status_code_high) AS max_prev_status_code_high,
    ROUND(SUM(prev_flag_bit1) / NULLIF(COUNT(CASE WHEN attr_key = 'trace' THEN 1 END), 0), 2) AS prev_flag_bit1_per_trace,
    COUNT_IF(attr_key = 'version') AS version_events
FROM ranked
GROUP BY CASE WHEN cumulative_prev_status_code_high >= 11 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT_IF(attr_key = 'version') > 1
ORDER BY cumulative_prev_status_code_high_tier DESC NULLS LAST;
