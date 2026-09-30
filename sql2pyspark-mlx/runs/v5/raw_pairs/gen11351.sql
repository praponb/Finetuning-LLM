SELECT full_name, INITCAP(TRIM(level)) AS level_title
FROM employees
ORDER BY full_name;
