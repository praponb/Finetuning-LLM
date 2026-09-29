SELECT id, dept_id, level, hire_date, LOWER(level) level_lower, CAST(salary AS INT) salary_int FROM employees WHERE full_name LIKE '%smi%';
