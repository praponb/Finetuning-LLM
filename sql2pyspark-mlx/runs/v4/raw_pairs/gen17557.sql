SELECT product_id, SUM(unit_price) AS total_unit_price, DENSE_RANK() OVER (ORDER BY SUM(unit_price) DESC) AS unit_price_rank
FROM order_items
GROUP BY product_id
ORDER BY product_id DESC
LIMIT 50;
