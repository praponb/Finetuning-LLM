WITH top_customers AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS pos FROM customers) SELECT id, city, signup_date FROM top_customers WHERE pos <= 2
