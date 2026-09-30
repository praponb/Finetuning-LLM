SELECT id, country, age, RANK() OVER (PARTITION BY country ORDER BY age) AS rnk FROM customers;
