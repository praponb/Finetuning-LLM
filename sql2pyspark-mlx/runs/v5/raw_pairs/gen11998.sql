SELECT
    id,
    dept_id,
    salary,
    level
FROM (
    SELECT
        id,
        dept_id,
        salary,
        level,
        RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) pos
    FROM employees
) t
WHERE pos <= 5
