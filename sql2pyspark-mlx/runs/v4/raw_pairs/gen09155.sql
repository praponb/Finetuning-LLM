SELECT
    CASE WHEN salary >= 127000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(salary), 2) AS avg_salary
FROM employees
GROUP BY salary_tier, year_start
ORDER BY salary_tier NULLS FIRST
