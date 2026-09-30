SELECT monthly_fee
FROM subscriptions
UNION ALL
SELECT unit_price AS monthly_fee
FROM order_items
ORDER BY monthly_fee;
