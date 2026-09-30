SELECT * FROM (SELECT id, country, signup_date, email, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS rn FROM customers) AS ranked WHERE rn <= 5
