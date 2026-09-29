SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary, COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count
FROM employees
GROUP BY dept_id
HAVING ROUND(STDDEV(salary), 2) > 40;
