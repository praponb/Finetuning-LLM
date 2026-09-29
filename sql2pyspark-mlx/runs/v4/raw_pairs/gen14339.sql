SELECT *
FROM employees
WHERE full_name IS NOT NULL AND is_remote = TRUE;
