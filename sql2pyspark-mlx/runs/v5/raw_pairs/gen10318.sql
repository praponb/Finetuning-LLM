SELECT
    dept_id,
    level,
    SUM(salary) FILTER (WHERE job_title = 'Designer') AS designer_total_salary,
    STDDEV_SAMP(salary) AS salary_stddev,
    COUNT_IF(job_title = 'Designer') AS designer_events,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN level = 'L3' THEN 1 END) AS l3_count,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN level = 'L3' THEN 1 END), 0), 2) AS salary_per_l3
FROM employees
GROUP BY ROLLUP (dept_id, level)
ORDER BY l3_count DESC NULLS LAST
