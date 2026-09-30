WITH filtered AS (
    SELECT *, UPPER(status) AS status_upper
    FROM payments
    WHERE paid_at >= DATE '2023-09-01'
      AND method IS NOT NULL
),
gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    SUM(is_new_session) FILTER (WHERE method = 'paypal') AS paypal_total_is_new_session,
    COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded_rows,
    COUNT_IF(status = 'SUCCESS') AS success_events,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles
FROM sessionized
GROUP BY ROLLUP (DATE_TRUNC('WEEK', paid_at), CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY refunded_rows NULLS LAST
