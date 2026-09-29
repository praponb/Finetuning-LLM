SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY signup_date, id) AS rn FROM customers) t WHERE rn = 1
