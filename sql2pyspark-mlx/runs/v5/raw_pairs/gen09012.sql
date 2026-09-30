SELECT *
FROM (
    SELECT
        manager_id,
        level,
        salary
    FROM employees
) src
PIVOT (
    AVG(salary) AS amount,
    COUNT(1) AS n
    FOR level IN ('L2', 'L5', 'L4')
)
