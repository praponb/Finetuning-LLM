WITH ranked_orders AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY discount DESC, id) AS rnk FROM orders) SELECT * FROM ranked_orders WHERE rnk = 1
