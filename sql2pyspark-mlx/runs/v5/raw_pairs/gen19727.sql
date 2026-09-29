WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(attr_key, TRUE) OVER (PARTITION BY host ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_attr_key,
        MAX(status_code) OVER w_running AS status_code_max_30m,
        AVG(status_code) OVER w_recent AS attr_key_avg_status_code
    FROM flattened
    WINDOW w_running AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY attr_key, host)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, host ORDER BY status_code DESC, id, attr_key) AS rank_in_group
        FROM scored
    ) ranked
    WHERE rank_in_group = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(status_code) OVER w_ordered AS running_count_status_code,
        AVG(status_code) OVER w_ordered AS running_avg_status_code,
        MAX(status_code) OVER w_ordered AS running_max_status_code,
        SUM(status_code) OVER w_ordered AS running_sum_status_code,
        AVG(status_code) OVER (ORDER BY attr_key_avg_status_code, id, attr_key ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS status_code_avg_5
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY host ORDER BY status_code_max_30m, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    status_code,
    attr_key,
    attr_value,
    status_code_max_30m,
    attr_key_avg_status_code,
    running_count_status_code,
    running_sum_status_code,
    status_code_avg_5,
    CASE WHEN attr_key RLIKE '^(region|version)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS attr_key_zone
FROM metrics
ORDER BY running_sum_status_code NULLS FIRST;
