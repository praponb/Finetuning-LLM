SELECT customers.email, t.total_monthly_fee
FROM customers
INNER JOIN (SELECT customer_id, SUM(monthly_fee) total_monthly_fee
    FROM subscriptions
    GROUP BY customer_id) t ON customers.id = t.customer_id
ORDER BY total_monthly_fee
