SELECT job_title, SUM(CASE WHEN level = 'L1' THEN 1 ELSE 0 END) AS l1_count, ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_designer
FROM employees
WHERE job_title = 'Analyst' AND level = 'L2'
GROUP BY 1;
