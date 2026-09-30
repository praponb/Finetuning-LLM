SELECT
    dept_id,
    PERCENTILE_APPROX(salary, 0.95) AS salary_p95,
    SIZE(COLLECT_SET(level)) AS num_levels,
    COUNT(CASE WHEN level = 'L5' THEN 1 END) AS l5_count,
    MAX(salary) AS max_salary,
    APPROX_PERCENTILE(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles
FROM employees
GROUP BY 1
