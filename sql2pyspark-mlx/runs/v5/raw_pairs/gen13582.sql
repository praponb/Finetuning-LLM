SELECT
    CASE WHEN salary >= 156000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN job_title = 'Manager' THEN 1 END), 0), 2) AS salary_per_manager,
    SUM(salary) AS total_salary,
    PERCENTILE_APPROX(salary, 0.9) AS salary_p90,
    STDDEV_SAMP(salary) AS salary_stddev,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN level = 'L1' THEN 1 END), 0), 2) AS salary_per_l1
FROM employees
GROUP BY CASE WHEN salary >= 156000 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', hire_date)
HAVING ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN level = 'L1' THEN 1 END), 0), 2) < 1000000
ORDER BY quarter_start DESC NULLS LAST
