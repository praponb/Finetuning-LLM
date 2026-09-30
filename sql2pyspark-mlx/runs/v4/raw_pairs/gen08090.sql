SELECT product_id, order_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM order_items
GROUP BY product_id, order_id
ORDER BY product_id DESC
