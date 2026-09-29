SELECT level, CAST(salary AS INT) AS salary_int FROM employees WHERE is_remote OR LOWER(email) LIKE '%enr%' ORDER BY salary_int DESC;
