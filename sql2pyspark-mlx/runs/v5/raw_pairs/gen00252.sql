WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
gaps AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, k) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM flattened
),
sessions_app_logs AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        k,
        attr_value,
        SUM(is_new_session) OVER (PARTITION BY service ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
matched_keys AS (
    SELECT service
    FROM sessions_app_logs
    EXCEPT
    SELECT service
    FROM app_logs
    WHERE HOUR(log_ts) BETWEEN 11 AND 14
),
reconciled AS (
    SELECT s.*
    FROM sessions_app_logs s
    INNER JOIN matched_keys k ON s.service = k.service
),
scored AS (
    SELECT
        id,
        host,
        log_ts,
        k,
        attr_value,
        session_id,
        CUME_DIST() OVER (PARTITION BY host, service ORDER BY session_id DESC) AS session_id_cume_dist,
        SUM(session_id) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS session_id_sum_15m,
        SUM(session_id) OVER (PARTITION BY service, k) AS service_sum_session_id
    FROM reconciled
),
enriched AS (
    SELECT
        id,
        log_ts,
        k,
        attr_value,
        session_id_cume_dist,
        session_id_sum_15m,
        SUM(session_id) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS session_id_sum_2h
    FROM scored
),
cleaned AS (
    SELECT
        id,
        log_ts,
        k,
        session_id_cume_dist,
        session_id_sum_2h,
        LENGTH(k) AS k_length
    FROM enriched
    WHERE k IS NOT NULL
)
SELECT
    id,
    session_id_sum_2h,
    k_length
FROM cleaned
