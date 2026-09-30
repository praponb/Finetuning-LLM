SELECT *
FROM (
    SELECT
        manager_id,
        level,
        salary
    FROM employees
) src
PIVOT (
    MAX(salary)
    FOR level IN ('L2' AS l2, 'L3' AS l3)
)
