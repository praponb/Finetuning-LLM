WITH deduped AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) = 1
)
SELECT
    manager_id,
    job_title,
    CASE WHEN salary >= 120000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    MIN(salary) AS min_salary,
    SUM(CASE WHEN job_title = 'Analyst' THEN salary ELSE 0 END) AS analyst_salary,
    COUNT_IF(job_title = 'Analyst') AS analyst_events,
    MAX(salary) FILTER (WHERE job_title = 'Engineer') AS engineer_max_salary,
    AVG(salary) FILTER (WHERE job_title = 'Manager') AS manager_avg_salary,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING(manager_id) AS grouping_id
FROM deduped
GROUP BY CUBE (manager_id, job_title, CASE WHEN salary >= 120000 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY engineer_max_salary;
