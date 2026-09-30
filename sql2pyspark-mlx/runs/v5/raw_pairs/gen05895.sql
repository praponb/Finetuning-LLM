SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS pos FROM employees) AS r WHERE pos <= 1
