SELECT
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    CASE WHEN salary >= 146000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    SUM(CASE WHEN level = 'L5' THEN salary ELSE 0 END) AS l5_salary,
    COUNT(CASE WHEN job_title = 'Director' THEN 1 END) AS director_count,
    ROUND(AVG(salary), 2) AS avg_salary,
    RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', hire_date) ORDER BY COUNT(CASE WHEN job_title = 'Director' THEN 1 END) DESC) AS director_count_rank
FROM employees
GROUP BY DATE_TRUNC('QUARTER', hire_date), DATE_TRUNC('WEEK', hire_date), CASE WHEN salary >= 146000 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY salary_tier NULLS LAST
