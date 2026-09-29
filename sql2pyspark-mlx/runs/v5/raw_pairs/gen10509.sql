SELECT *
FROM (
    SELECT
        id,
        level,
        hire_date,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS rn
    FROM employees
) AS ranked
WHERE rn <= 2
ORDER BY rn DESC;
