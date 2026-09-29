SELECT id, full_name, email, ROUND(salary, 1) AS salary_rounded FROM employees WHERE level IS NULL AND full_name LIKE '%Car%';
