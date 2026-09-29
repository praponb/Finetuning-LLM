SELECT
    id,
    is_remote,
    level,
    CASE WHEN salary >= 138000 THEN 'High' WHEN salary >= 45000 THEN 'Medium' ELSE 'Low' END salary_tier
FROM employees
WHERE level <> 'L2'
ORDER BY id
