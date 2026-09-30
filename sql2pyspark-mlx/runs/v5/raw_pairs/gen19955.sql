SELECT unit_price
FROM order_items
UNION
SELECT shipping_fee
FROM orders
ORDER BY unit_price;
