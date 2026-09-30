SELECT id, city, age, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC, id) AS rn FROM customers WHERE MONTH(signup_date) IN (1, 4, 9)
