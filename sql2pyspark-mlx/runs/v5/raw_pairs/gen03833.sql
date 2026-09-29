SELECT id, segment, signup_date, age, LAG(age, 1) OVER (PARTITION BY segment ORDER BY signup_date, id) AS prev_age FROM customers ORDER BY 3
