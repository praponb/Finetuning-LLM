WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        HOUR(log_ts) AS log_ts_hour,
        COALESCE(attr_key, 'DIRECT') AS resolved_attr_key
    FROM lines
    WHERE attr_key IS DISTINCT FROM 'trace'
      AND attr_key IN ('trace', 'region', 'version')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_running AS log_ts_hour_percent_rank,
        SUM(log_ts_hour) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS log_ts_hour_sum_1h,
        MIN(log_ts_hour) OVER w_ordered AS attr_key_min_log_ts_hour,
        CUME_DIST() OVER (ORDER BY log_ts_hour) AS log_ts_hour_cume_dist,
        SUM(log_ts_hour) OVER w_ordered AS attr_key_sum_log_ts_hour
    FROM filtered
    WINDOW w_running AS (ORDER BY log_ts_hour DESC NULLS FIRST), w_ordered AS (PARTITION BY attr_key)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(attr_key_sum_log_ts_hour, 1) AS attr_key_sum_log_ts_hour_rounded,
        FLOOR(log_ts_hour_sum_1h) AS log_ts_hour_sum_1h_whole
    FROM metrics
    WHERE log_ts >= TO_DATE('2025-06-01')
      AND log_ts_hour_sum_1h >= 358
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        attr_key,
        attr_value,
        log_ts_hour,
        resolved_attr_key,
        log_ts_hour_sum_1h,
        attr_key_min_log_ts_hour,
        attr_key_sum_log_ts_hour_rounded,
        log_ts_hour_sum_1h_whole,
        COUNT(log_ts_hour_percent_rank) OVER w_grp AS attr_key_count_log_ts_hour_percent_rank,
        STDDEV_SAMP(log_ts_hour_sum_1h) OVER w_grp AS attr_key_stddev_samp_log_ts_hour_sum_1h
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY attr_key)
),
with_windows_app_logs AS (
    -- Ranking and gap features
    SELECT
        attr_key_sum_log_ts_hour_rounded,
        log_ts_hour_sum_1h_whole,
        attr_key_count_log_ts_hour_percent_rank,
        attr_key_stddev_samp_log_ts_hour_sum_1h,
        STDDEV_SAMP(attr_key_min_log_ts_hour) OVER (PARTITION BY attr_key) AS attr_key_stddev_samp_attr_key_min_log_ts_hour,
        SUM(attr_key_sum_log_ts_hour_rounded) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS attr_key_sum_log_ts_hour_rounded_sum_2h
    FROM with_windows
)
SELECT
    attr_key_count_log_ts_hour_percent_rank,
    attr_key_stddev_samp_log_ts_hour_sum_1h,
    attr_key_stddev_samp_attr_key_min_log_ts_hour,
    attr_key_sum_log_ts_hour_rounded_sum_2h
FROM with_windows_app_logs
