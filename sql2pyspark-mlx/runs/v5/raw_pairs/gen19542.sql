SELECT
    manager_id,
    job_title,
    intern_count,
    min_salary
FROM (
    SELECT
        manager_id,
        job_title,
        SUM(CASE WHEN job_title = 'Intern' THEN 1 ELSE 0 END) AS intern_count,
        MIN(salary) AS min_salary
    FROM employees
    GROUP BY manager_id, job_title
) AS agg
WHERE intern_count > 33;
