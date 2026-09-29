SELECT COUNT(*) total_count, ROUND(AVG(unit_price), 2) avg_unit_price, COUNT(DISTINCT unit_price) unique_products FROM order_items WHERE product_id = 24 ORDER BY avg_unit_price DESC;
