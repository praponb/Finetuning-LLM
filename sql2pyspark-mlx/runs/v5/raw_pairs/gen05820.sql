SELECT 'product' kind, cost
FROM products
WHERE product_name NOT LIKE 'D%'
UNION
SELECT 'subscription' kind, monthly_fee
FROM subscriptions
ORDER BY cost
