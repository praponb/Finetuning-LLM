SELECT hire_date FROM employees WHERE salary <= 67000 UNION SELECT hire_date FROM employees WHERE manager_id IS NOT NULL;
