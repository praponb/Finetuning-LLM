WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS rnk FROM accounts) SELECT id, customer_id, opened_date FROM numbered WHERE rnk <= 5;
