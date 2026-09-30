WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
with_windows AS (
    SELECT
        id,
        service,
        log_ts,
        status_code,
        k,
        value,
        SUM(status_code) OVER (PARTITION BY service, k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_sum_1h,
        SUM(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_sum_1h_2,
        COUNT(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_count_2h
    FROM flattened
),
cleaned AS (
    SELECT
        service,
        log_ts,
        status_code,
        k,
        status_code_sum_1h,
        status_code_count_2h,
        status_code_sum_1h - status_code_count_2h AS status_code_sum_1h_minus_status_code_count_2h
    FROM with_windows
    WHERE status_code_sum_1h IS NULL
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    CASE WHEN status_code_sum_1h >= 1603 THEN 'LARGE' ELSE 'SMALL' END AS status_code_sum_1h_tier,
    APPROX_PERCENTILE(status_code_sum_1h, 0.9) AS status_code_sum_1h_p90,
    COUNT(CASE WHEN service = 'checkout' THEN 1 END) AS checkout_count
FROM cleaned
GROUP BY DATE_TRUNC('HOUR', log_ts), CASE WHEN status_code_sum_1h >= 1603 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY hour_window DESC
