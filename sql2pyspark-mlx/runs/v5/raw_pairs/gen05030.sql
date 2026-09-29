WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        log_ts,
        context,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        context['version'] AS version
    FROM app_logs
)
-- Window metrics over several different partitions
SELECT
    *,
    COUNT(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) status_code_high_count_1h,
    AVG(status_code_high) OVER (ORDER BY log_ts, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) status_code_high_avg_5
FROM flattened_attrs
