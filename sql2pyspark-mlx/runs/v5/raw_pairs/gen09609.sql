SELECT monthly_fee, 'subscription' AS kind
FROM subscriptions
UNION
SELECT unit_price, 'order_item' AS kind
FROM order_items
