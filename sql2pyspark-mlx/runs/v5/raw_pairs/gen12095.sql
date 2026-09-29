SELECT customer_id, SUM(discount) AS total_discount, SUM(SUM(discount)) OVER (ORDER BY customer_id) AS cumulative_discount
FROM orders
GROUP BY customer_id
ORDER BY total_discount, customer_id;
