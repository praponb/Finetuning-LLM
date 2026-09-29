SELECT order_id, avg_quantity
FROM (
    SELECT order_id, AVG(quantity) AS avg_quantity
    FROM order_items
    WHERE order_id = 3
    GROUP BY order_id
) AS agg
WHERE avg_quantity < 7.1
ORDER BY avg_quantity DESC, order_id DESC;
