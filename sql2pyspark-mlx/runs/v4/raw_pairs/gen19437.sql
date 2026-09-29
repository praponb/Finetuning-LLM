SELECT monthly_fee
FROM subscriptions
WHERE monthly_fee >= 230
UNION
SELECT shipping_fee
FROM orders
WHERE shipping_fee < 19
ORDER BY monthly_fee;
