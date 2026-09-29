SELECT
    id,
    level,
    hire_date,
    full_name
FROM (
    SELECT
        id,
        level,
        hire_date,
        full_name,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS pos
    FROM employees
) AS ranked
WHERE pos <= 3
ORDER BY full_name;
