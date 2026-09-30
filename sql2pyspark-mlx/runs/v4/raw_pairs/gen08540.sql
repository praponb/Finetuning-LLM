SELECT product_id, unique_products, quantity_count
FROM (
    SELECT product_id, COUNT(DISTINCT unit_price) AS unique_products, COUNT(quantity) AS quantity_count
    FROM order_items
    GROUP BY product_id
) AS t
WHERE unique_products >= 9
ORDER BY 2 DESC
