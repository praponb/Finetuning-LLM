WITH subscriptions_by_customer AS (
    SELECT customer_id, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM subscriptions
    GROUP BY customer_id
)
SELECT c.city, c.email, sbc.stddev_monthly_fee
FROM customers c
INNER JOIN subscriptions_by_customer sbc ON c.id = sbc.customer_id
ORDER BY c.city DESC
