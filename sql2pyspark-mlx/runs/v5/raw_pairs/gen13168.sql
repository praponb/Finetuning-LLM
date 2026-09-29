SELECT job_title, SUM(salary) total_salary, COUNT(*) cnt, ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total FROM employees GROUP BY job_title
