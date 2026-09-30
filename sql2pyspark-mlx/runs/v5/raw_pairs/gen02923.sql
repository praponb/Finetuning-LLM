SELECT *
FROM (
    SELECT
        manager_id,
        dept_id,
        level,
        salary
    FROM employees
) src
PIVOT (
    MAX(salary) AS amount,
    COUNT(1) AS item_count
    FOR level IN ('L2', 'L4')
);
