SELECT
    is_remote,
    COUNT(CASE WHEN level = 'L5' THEN 1 END) AS l5_count,
    COUNT(DISTINCT salary) AS unique_depts,
    MAX(salary) AS max_salary
FROM employees
GROUP BY is_remote;
