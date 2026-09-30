WITH customer_summary AS (
    SELECT customer_id, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range, COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) AS expired_count
    FROM subscriptions
    WHERE customer_id = 40
    GROUP BY customer_id
)
SELECT
    c.email,
    c.age,
    cs.monthly_fee_range,
    cs.expired_count
FROM customers c
LEFT OUTER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.expired_count >= 6;
