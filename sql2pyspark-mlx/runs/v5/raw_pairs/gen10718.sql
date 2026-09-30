SELECT * FROM (SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, city, age FROM customers) PIVOT (MAX(age) FOR city IN ('Berlin', 'Singapore', 'Chiang Mai', 'New York'));
