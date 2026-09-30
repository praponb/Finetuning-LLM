WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        status,
        ROUND(amount * 100, 2) AS amount_adjusted,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM payments
    WHERE paid_at >= DATE '2023-08-01'
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount_adjusted >= 67 THEN 'HIGH' ELSE 'LOW' END AS amount_adjusted_tier,
    COUNT_IF(status = 'REFUNDED') AS refunded_events,
    COUNT(CASE WHEN method = 'promptpay' THEN 1 END) AS promptpay_count,
    APPROX_PERCENTILE(amount_adjusted, 0.9) AS amount_adjusted_p90
FROM cleaned
GROUP BY CASE WHEN amount_adjusted >= 67 THEN 'HIGH' ELSE 'LOW' END
ORDER BY amount_adjusted_p90 DESC NULLS FIRST;
