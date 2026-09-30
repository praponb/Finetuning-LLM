SELECT id, credit_limit, DENSE_RANK() OVER (ORDER BY credit_limit) AS dense_rnk FROM customers
