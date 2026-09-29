SELECT
    product_id,
    SUM(unit_price) total_unit_price,
    COUNT(*) cnt,
    SUM(SUM(unit_price)) OVER (ORDER BY product_id) AS cumulative_unit_price
FROM order_items
GROUP BY product_id
ORDER BY cumulative_unit_price DESC;
