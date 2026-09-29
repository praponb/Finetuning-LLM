WITH top_customers AS (SELECT id, country, age, signup_date, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS rnk FROM customers) SELECT * FROM top_customers WHERE rnk <= 1;
