SELECT amount
FROM payments
UNION ALL
SELECT unit_price
FROM order_items
