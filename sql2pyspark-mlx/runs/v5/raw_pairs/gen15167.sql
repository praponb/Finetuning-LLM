SELECT
    c.id,
    c.country,
    c.city,
    t.monthly_fee_range,
    t.unique_customers
FROM customers c
INNER JOIN (SELECT customer_id, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range, COUNT(DISTINCT monthly_fee) AS unique_customers
    FROM subscriptions
    WHERE monthly_fee >= 90
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.unique_customers > 7
ORDER BY c.city DESC
