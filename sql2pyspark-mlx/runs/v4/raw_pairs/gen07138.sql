WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, value
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_avg_15m,
        COUNT(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_count_30m,
        AVG(status_code) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_avg_30m,
        CUME_DIST() OVER (PARTITION BY service ORDER BY status_code DESC) AS status_code_cume_dist,
        RANK() OVER (PARTITION BY attr_key ORDER BY status_code) AS status_code_rank
    FROM flattened
),
scored_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code_count_30m NULLS LAST) AS status_code_count_30m_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY status_code_cume_dist NULLS LAST) AS status_code_cume_dist_percent_rank,
        AVG(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_avg_15m_2,
        AVG(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_avg_15m_3
    FROM scored
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(status_code_avg_15m_2) OVER w_running AS status_code_avg_15m_2_sum_1h,
        MAX(status_code_count_30m) OVER w_running_2 AS attr_key_max_status_code_count_30m
    FROM scored_app_logs
    WINDOW w_running AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY attr_key)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, service
    FROM with_windows
    EXCEPT ALL
    SELECT id, service
    FROM app_logs
    WHERE host IN ('h2', 'h3')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id AND s.service = k.service
),
matched_keys_2 AS (
    -- Set intersection on keys
    SELECT id, level
    FROM kept_rows
    INTERSECT ALL
    SELECT id, level
    FROM app_logs
    WHERE HOUR(log_ts) BETWEEN 11 AND 17
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN matched_keys_2 k ON s.id = k.id AND s.level = k.level
)
SELECT
    id,
    service,
    log_ts,
    status_code,
    attr_key,
    status_code_count_30m,
    status_code_avg_30m,
    status_code_rank,
    status_code_cume_dist_percent_rank
FROM reconciled;
