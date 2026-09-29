SELECT id, country, age, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS row_num FROM customers WHERE credit_limit BETWEEN 4800 AND 8800 ORDER BY id;
