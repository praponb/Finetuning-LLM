SELECT store_id, SUM(discount) total_discount, RANK() OVER (ORDER BY SUM(discount) DESC) AS discount_rank FROM orders GROUP BY store_id;
