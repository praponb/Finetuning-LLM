WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        level,
        status_code,
        attr_value,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
matched_keys AS (
    -- Set difference on keys
    SELECT level
    FROM parsed
    EXCEPT ALL
    SELECT level
    FROM app_logs
    WHERE latency_ms BETWEEN 959 AND 1414
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM parsed s
    JOIN matched_keys k ON s.level = k.level
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        level,
        status_code,
        attr_value,
        flag_bit2,
        status_code_high,
        SUM(status_code_high) OVER w_part AS level_sum_status_code_high,
        COUNT(flag_bit2) OVER w_part AS level_count_flag_bit2
    FROM reconciled
    WINDOW w_part AS (PARTITION BY level)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(level) AS level_upper
    FROM with_windows
    WHERE status_code >= 287
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        level,
        status_code,
        attr_value,
        flag_bit2,
        status_code_high,
        level_sum_status_code_high,
        level_count_flag_bit2,
        level_upper,
        CUME_DIST() OVER (PARTITION BY level ORDER BY level_count_flag_bit2) AS level_count_flag_bit2_cume_dist,
        RANK() OVER (PARTITION BY level ORDER BY status_code) AS status_code_rank,
        RANK() OVER (PARTITION BY level ORDER BY level_sum_status_code_high DESC NULLS LAST) AS level_sum_status_code_high_rank
    FROM cleaned
)
SELECT
    level,
    status_code,
    flag_bit2,
    status_code_high,
    level_sum_status_code_high,
    level_count_flag_bit2,
    status_code_rank
FROM enriched
ORDER BY level_count_flag_bit2 DESC
