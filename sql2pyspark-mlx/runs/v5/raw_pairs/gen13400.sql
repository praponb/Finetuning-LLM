SELECT
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    dept_id,
    PERCENTILE_APPROX(salary, 0.9) AS salary_p90,
    SUM(CASE WHEN level = 'L2' THEN salary ELSE 0 END) AS l2_salary,
    AVG(salary) AS avg_salary,
    COUNT_IF(level = 'L3') AS l3_events,
    COUNT(*) FILTER (WHERE job_title = 'Manager') AS manager_rows,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', hire_date) ORDER BY COUNT(*) FILTER (WHERE job_title = 'Manager') DESC) AS manager_rows_dense_rank
FROM employees
GROUP BY DATE_TRUNC('QUARTER', hire_date), dept_id
ORDER BY quarter_start NULLS LAST
