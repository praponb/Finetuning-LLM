SELECT *
FROM (
    SELECT
        manager_id,
        job_title,
        COUNT(CASE WHEN level = 'L1' THEN 1 END) AS l1_count,
        MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    WHERE level = 'L2'
    GROUP BY manager_id, job_title
) AS agg
WHERE salary_range < 52
