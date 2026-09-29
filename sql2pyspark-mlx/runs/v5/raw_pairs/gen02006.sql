SELECT
    manager_id,
    salary,
    level,
    CASE WHEN level IN ('L4', 'L2') THEN 'Core' ELSE 'Other' END AS level_group
FROM employees
WHERE level = 'L4'
