SELECT revenue
FROM store_sales
INTERSECT
SELECT shipping_fee
FROM orders
