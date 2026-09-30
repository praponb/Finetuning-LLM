SELECT dept_id, SUM(salary) AS total_salary, ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total
FROM employees
GROUP BY dept_id
ORDER BY 1 DESC;
