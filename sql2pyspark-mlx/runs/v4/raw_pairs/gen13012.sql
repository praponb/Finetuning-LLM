SELECT id, level, salary FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS rnk FROM employees) AS ranked WHERE rnk <= 2
