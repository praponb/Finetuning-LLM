SELECT
    dept_id,
    is_remote,
    ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_designer,
    MIN(hire_date) min_hire_date
FROM employees
GROUP BY dept_id, is_remote
