SELECT *, INITCAP(TRIM(full_name)) AS full_name_title, UPPER(level) AS level_upper
FROM employees
WHERE hire_date >= '2023-05-01';
