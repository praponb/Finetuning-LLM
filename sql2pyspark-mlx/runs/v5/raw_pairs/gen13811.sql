SELECT id, is_remote, DATE_ADD(hire_date, 80) AS hire_date_plus_80d
FROM employees
WHERE level IS NOT NULL;
