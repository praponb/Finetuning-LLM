WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, v
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(status_code) OVER w_hist AS status_code_sum_30m,
        COUNT(latency_ms) OVER (PARTITION BY key, level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_count_1h
    FROM exploded
    WINDOW w_hist AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(service, ', ', key) AS service_key
    FROM enriched
    WHERE log_ts > TO_TIMESTAMP('2025-06-02 01:00:00')
      AND key IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        key,
        status_code_sum_30m,
        latency_ms_count_1h,
        service_key,
        TRIM(service) AS clean_service,
        UPPER(service) AS service_upper
    FROM filtered
    WHERE log_ts > TO_DATE('2025-06-01')
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(latency_ms_count_1h) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_count_1h_max_2h,
        MAX(latency_ms) OVER w_hist AS latency_ms_max_1h,
        CUME_DIST() OVER (PARTITION BY service ORDER BY latency_ms NULLS FIRST) AS latency_ms_cume_dist
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(latency_ms_count_1h_max_2h / SUM(latency_ms_count_1h_max_2h) OVER (PARTITION BY key), 4) AS latency_ms_count_1h_max_2h_share,
        ROUND(latency_ms_count_1h / SUM(latency_ms_count_1h) OVER (PARTITION BY service), 4) AS latency_ms_count_1h_share,
        COUNT(status_code_sum_30m) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_sum_30m_count_2h
    FROM enriched_app_logs
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        service,
        latency_ms_count_1h_share,
        latency_ms_cume_dist,
        latency_ms_count_1h_max_2h_share
    FROM metrics
) src
UNPIVOT (
    metric_value FOR measure IN (latency_ms_count_1h_share, latency_ms_cume_dist, latency_ms_count_1h_max_2h_share)
);
