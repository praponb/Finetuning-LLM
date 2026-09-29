WITH gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM payments
),
sessions_payments AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    PERCENTILE_APPROX(burst_id, ARRAY(0.25, 0.5, 0.75)) AS burst_id_quartiles,
    PERCENTILE_APPROX(burst_id, ARRAY(0.25, 0.5, 0.75)) AS burst_id_quartiles_2,
    COUNT_IF(method = 'promptpay') AS promptpay_events,
    PERCENTILE_APPROX(amount, 0.9) AS amount_p90,
    MAX(burst_id) FILTER (WHERE status = 'SUCCESS') AS success_max_burst_id,
    GROUPING(DATE_TRUNC('MONTH', paid_at)) AS grouping_id
FROM sessions_payments
GROUP BY CUBE (DATE_TRUNC('MONTH', paid_at), DATE_TRUNC('QUARTER', paid_at))
HAVING COUNT_IF(method = 'promptpay') >= 1
ORDER BY amount_p90 DESC NULLS FIRST;
