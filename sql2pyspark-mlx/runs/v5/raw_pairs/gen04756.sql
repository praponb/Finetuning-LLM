SELECT * FROM (SELECT id, manager_id, salary, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS rn FROM employees) AS r WHERE rn <= 5 ORDER BY salary DESC;
