SELECT
    CASE WHEN salary >= 127000 THEN 'high' WHEN salary >= 43000 THEN 'mid' ELSE 'low' END salary_bucket,
    SUM(salary) total_salary,
    COUNT(*) cnt,
    RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk
FROM employees
WHERE full_name LIKE '%FRAN%'
GROUP BY CASE WHEN salary >= 127000 THEN 'high' WHEN salary >= 43000 THEN 'mid' ELSE 'low' END
