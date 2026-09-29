SELECT id, customer_id, opened_date, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY opened_date) AS dense_rnk FROM accounts ORDER BY 1 DESC;
