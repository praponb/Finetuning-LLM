SELECT level, UPPER(level) AS level_upper, SUBSTRING(full_name, 1, 2) AS full_name_prefix
FROM employees
WHERE level IN ('L4', 'L3')
