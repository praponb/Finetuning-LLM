SELECT id, manager_id, LOWER(email) email_lower FROM employees WHERE full_name LIKE 'Ka%' AND dept_id IN (8, 19, 25) AND salary BETWEEN 75000 AND 100000;
