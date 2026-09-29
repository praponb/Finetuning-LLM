SELECT id, full_name, email FROM employees WHERE salary > 93000 AND (email IS NOT NULL OR email IS NULL)
