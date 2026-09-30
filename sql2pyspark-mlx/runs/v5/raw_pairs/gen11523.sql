SELECT cost, 'product' AS source
FROM products
UNION
SELECT monthly_fee, 'subscription' AS source
FROM subscriptions
ORDER BY cost
