SELECT id, email, dept_id, CAST(salary AS INT) AS salary_int FROM employees ORDER BY email, id DESC LIMIT 1
