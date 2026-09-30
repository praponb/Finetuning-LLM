SELECT CASE WHEN salary >= 153000 THEN 'high' WHEN salary >= 72000 THEN 'mid' ELSE 'low' END AS salary_bucket, SUM(salary) AS total_salary, SUM(SUM(salary)) OVER (ORDER BY CASE WHEN salary >= 153000 THEN 'high' WHEN salary >= 72000 THEN 'mid' ELSE 'low' END) AS cumulative_salary
FROM employees
GROUP BY CASE WHEN salary >= 153000 THEN 'high' WHEN salary >= 72000 THEN 'mid' ELSE 'low' END
ORDER BY cumulative_salary DESC;
