SELECT store_id, SUM(discount) AS total_discount, SUM(SUM(discount)) OVER (ORDER BY store_id) AS cumulative_discount
FROM orders
GROUP BY store_id;
