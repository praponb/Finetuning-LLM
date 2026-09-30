SELECT id, dept_id, salary, is_remote FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rn FROM employees) AS t WHERE rn <= 2;
