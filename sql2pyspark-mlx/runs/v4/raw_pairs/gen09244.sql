SELECT id, city, signup_date, country FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS row_num FROM customers) AS ranked WHERE row_num <= 3;
