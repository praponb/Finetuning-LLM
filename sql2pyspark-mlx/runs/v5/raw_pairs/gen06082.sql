SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount NULLS FIRST, id) AS latest_rank
FROM orders
QUALIFY latest_rank = 1
