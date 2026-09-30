SELECT *
FROM (
    SELECT product_id, COUNT(*) AS order_item_count, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    WHERE product_id IN (6, 10, 34, 38)
    GROUP BY product_id
) AS agg
WHERE order_item_count > 8
ORDER BY 3 DESC;
