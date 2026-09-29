SELECT *, ROW_NUMBER() OVER (PARTITION BY level, manager_id ORDER BY salary, id) AS rn
FROM employees
QUALIFY rn = 1
