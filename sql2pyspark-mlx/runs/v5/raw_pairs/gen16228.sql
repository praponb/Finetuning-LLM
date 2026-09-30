SELECT revenue
FROM store_sales
UNION
SELECT shipping_fee
FROM orders
ORDER BY revenue DESC;
