SELECT id, city, age, RANK() OVER (PARTITION BY city ORDER BY age) rnk FROM customers
