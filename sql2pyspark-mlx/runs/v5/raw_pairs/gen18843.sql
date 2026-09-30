SELECT cost value
FROM products
WHERE SIZE(tags) >= 2
EXCEPT
SELECT monthly_fee value
FROM subscriptions
ORDER BY value;
