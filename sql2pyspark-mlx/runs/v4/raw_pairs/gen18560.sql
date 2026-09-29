WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        v,
        SUM(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_sum_2h,
        SUM(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_sum_1h,
        COUNT(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_count_2h,
        DENSE_RANK() OVER (ORDER BY status_code DESC) AS status_code_dense_rank
    FROM lines
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(status_code) AS status_code_whole
    FROM metrics
    WHERE status_code_count_2h BETWEEN 10 AND 11
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        k,
        service,
        status_code_sum_2h
    FROM filtered
) src
PIVOT (
    SUM(status_code_sum_2h) AS amount,
    COUNT(1) AS cnt
    FOR service IN ('billing' AS billing, 'checkout' AS checkout)
);
