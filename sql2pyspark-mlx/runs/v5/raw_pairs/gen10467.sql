SELECT unit_price
FROM order_items
UNION ALL
SELECT monthly_fee
FROM subscriptions
ORDER BY unit_price;
