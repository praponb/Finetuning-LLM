SELECT order_date
FROM orders
WHERE store_id IS NULL
UNION
SELECT order_date
FROM orders
WHERE shipping_fee >= 18;
