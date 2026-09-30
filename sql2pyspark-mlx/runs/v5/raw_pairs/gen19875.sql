SELECT
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    level,
    STDDEV_SAMP(salary) AS salary_stddev,
    SIZE(COLLECT_SET(level)) AS num_levels,
    SUM(CASE WHEN level = 'L2' THEN salary ELSE 0 END) AS l2_salary
FROM employees
GROUP BY ROLLUP (DATE_TRUNC('WEEK', hire_date), level);
