SELECT order_id, product_id, ROUND(STDDEV(quantity), 2) stddev_quantity
FROM order_items
WHERE quantity BETWEEN 3 AND 6
GROUP BY order_id, product_id
ORDER BY order_id DESC, stddev_quantity DESC;
