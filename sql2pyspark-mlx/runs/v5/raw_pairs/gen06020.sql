SELECT id, level, salary
FROM (
    SELECT
        id,
        level,
        salary,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS pos
    FROM employees
    WHERE full_name IS NOT NULL
) AS r
WHERE pos = 1
