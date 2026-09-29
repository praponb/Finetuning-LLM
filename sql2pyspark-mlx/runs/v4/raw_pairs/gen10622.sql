WITH recent_payments AS (
    SELECT *
    FROM payments
    WHERE order_id = 18 AND order_id = 24 AND method IN ('bank_transfer', 'cash', 'promptpay')
),
payments_stats AS (
    SELECT method, ROUND((100 * SUM(CASE WHEN method = 'paypal' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_paypal
    FROM recent_payments
    GROUP BY method
)
SELECT method, pct_paypal
FROM payments_stats
ORDER BY method DESC
