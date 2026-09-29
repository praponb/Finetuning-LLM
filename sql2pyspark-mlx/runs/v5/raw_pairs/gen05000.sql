SELECT
    id,
    level,
    hire_date,
    full_name,
    FIRST_VALUE(full_name) OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS first_full_name
FROM employees
WHERE UPPER(email) LIKE '%ANY%'
ORDER BY level ASC;
