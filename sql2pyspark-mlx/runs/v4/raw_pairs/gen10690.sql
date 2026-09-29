SELECT
    CASE WHEN salary >= 147000 THEN 'high' WHEN salary >= 50000 THEN 'mid' ELSE 'low' END AS salary_bucket,
    SUM(salary) AS total_salary,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(salary) DESC) AS salary_rank
FROM employees
GROUP BY CASE WHEN salary >= 147000 THEN 'high' WHEN salary >= 50000 THEN 'mid' ELSE 'low' END
ORDER BY cnt DESC;
