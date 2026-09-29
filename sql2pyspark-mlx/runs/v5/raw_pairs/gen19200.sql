SELECT is_remote, pct_l2, avg_salary
FROM (
    SELECT is_remote, ROUND((100 * SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l2, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY is_remote
) AS agg
WHERE avg_salary >= 106000
ORDER BY pct_l2 DESC
