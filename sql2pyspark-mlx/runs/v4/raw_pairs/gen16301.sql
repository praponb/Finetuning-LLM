SELECT
    dept_id,
    COUNT_IF(job_title = 'Director') AS director_events,
    AVG(salary) AS avg_salary,
    COUNT(CASE WHEN level = 'L3' THEN 1 END) AS l3_count
FROM employees
GROUP BY ROLLUP (dept_id);
