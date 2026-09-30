SELECT monthly_fee AS value
FROM subscriptions
UNION ALL
SELECT unit_price AS value
FROM order_items
WHERE unit_price BETWEEN 260 AND 320
