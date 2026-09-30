SELECT id, city, signup_date, age, LEAD(age, 1) OVER (PARTITION BY city ORDER BY signup_date, id) next_age FROM customers;
