SELECT shipping_fee
FROM orders
EXCEPT
SELECT monthly_fee
FROM subscriptions
ORDER BY shipping_fee
