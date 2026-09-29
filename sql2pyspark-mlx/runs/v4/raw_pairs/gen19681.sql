SELECT
    order_id,
    product_id,
    MAX(unit_price) - MIN(unit_price) AS unit_price_range,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
GROUP BY order_id, product_id
ORDER BY 4 DESC;
