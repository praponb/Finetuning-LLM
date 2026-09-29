WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
),
unpacked AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged
    FROM lines
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level, attr_key ORDER BY log_ts DESC, id, attr_key) AS row_num
    FROM unpacked
    QUALIFY row_num = 1
),
gaps AS (
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
valid AS (
    SELECT
        *,
        SUBSTRING(service, 1, 2) service_prefix,
        INITCAP(TRIM(service)) service_title
    FROM islands
    WHERE log_ts > TO_DATE('2025-06-01')
),
matched_keys AS (
    SELECT id, service
    FROM valid
    EXCEPT
    SELECT id, service
    FROM app_logs
    WHERE host IN ('h2', 'h1', 'h3')
),
verified AS (
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id AND s.service = k.service
),
kept_keys AS (
    SELECT service, id
    FROM verified
    EXCEPT DISTINCT
    SELECT service, id
    FROM app_logs
    WHERE level IN ('ERROR', 'DEBUG')
),
verified_app_logs AS (
    SELECT s.*
    FROM verified s
    INNER JOIN kept_keys k ON s.service = k.service AND s.id = k.id
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    service,
    COUNT(DISTINCT attr_key) AS distinct_attr_keys,
    ROUND(AVG(is_new_burst), 2) AS avg_is_new_burst,
    COUNT_IF(attr_key = 'region') AS region_events,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', log_ts) ORDER BY COUNT(DISTINCT attr_key) DESC) AS distinct_attr_keys_dense_rank
FROM verified_app_logs
GROUP BY DATE_TRUNC('HOUR', log_ts), service
ORDER BY hour_window DESC NULLS FIRST
