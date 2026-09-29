SELECT order_id, SUM(amount) AS total_amount, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM payments GROUP BY order_id ORDER BY cnt DESC
