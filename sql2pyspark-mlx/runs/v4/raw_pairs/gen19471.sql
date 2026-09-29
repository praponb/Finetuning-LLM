SELECT job_title, ROUND((100 * SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l5
FROM employees
GROUP BY job_title
HAVING ROUND((100 * SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 47
