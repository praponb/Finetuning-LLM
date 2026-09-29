SELECT
    order_id,
    product_id,
    COUNT(*) AS cnt,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    COUNT(*) AS num_order_items
FROM order_items
GROUP BY CUBE(order_id, product_id)
ORDER BY cnt DESC, num_order_items DESC
