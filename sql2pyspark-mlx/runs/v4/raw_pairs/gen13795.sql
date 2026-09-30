SELECT id, age, segment, RANK() OVER (ORDER BY age) AS rnk, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS age_row FROM customers;
