SELECT id, store_id, amount, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY amount) AS dense_rnk FROM orders ORDER BY dense_rnk DESC;
