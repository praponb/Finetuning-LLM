SELECT amount
FROM orders
EXCEPT
SELECT unit_price
FROM order_items
WHERE order_id = 12;
