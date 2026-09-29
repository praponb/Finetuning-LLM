WITH customer_totals AS (
    SELECT customer_id, COUNT(monthly_fee) AS monthly_fee_count, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    GROUP BY customer_id
)
SELECT
    c.id,
    c.is_active,
    ct.monthly_fee_count,
    ct.monthly_fee_range
FROM customers c
LEFT JOIN customer_totals ct ON c.id = ct.customer_id
WHERE ct.monthly_fee_count > 6
ORDER BY ct.monthly_fee_count DESC
