WITH subscriptions_by_customer AS (
    SELECT customer_id, MAX(monthly_fee) AS max_monthly_fee
    FROM subscriptions
    GROUP BY customer_id
)
SELECT customers.name, customers.email, subscriptions_by_customer.max_monthly_fee
FROM customers
JOIN subscriptions_by_customer ON customers.id = subscriptions_by_customer.customer_id
WHERE subscriptions_by_customer.max_monthly_fee > 110
