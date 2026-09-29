SELECT id, hire_date, is_remote, DATEDIFF(CURRENT_DATE(), hire_date) days_since_hire_date FROM employees WHERE full_name IS NULL;
