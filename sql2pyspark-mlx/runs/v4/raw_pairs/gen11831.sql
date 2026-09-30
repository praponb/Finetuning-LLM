SELECT id, segment, age, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS row_num FROM customers
