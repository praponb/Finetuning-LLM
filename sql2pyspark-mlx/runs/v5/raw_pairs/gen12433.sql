WITH kept_keys AS (
    -- Set difference on keys
    SELECT job_title, manager_id
    FROM employees
    EXCEPT
    SELECT job_title, manager_id
    FROM employees
    WHERE job_title <> 'Manager'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN kept_keys k ON s.job_title = k.job_title AND s.manager_id = k.manager_id
)
-- Grouped metrics
SELECT
    CASE WHEN salary >= 130000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    job_title,
    COUNT_IF(job_title = 'Intern') AS intern_events,
    COUNT(CASE WHEN level = 'L4' THEN 1 END) AS l4_count,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) AS intern_count,
    RANK() OVER (PARTITION BY CASE WHEN salary >= 130000 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(CASE WHEN level = 'L4' THEN 1 END) DESC) AS l4_count_rank
FROM reconciled
GROUP BY CASE WHEN salary >= 130000 THEN 'LARGE' ELSE 'SMALL' END, job_title
HAVING intern_events > 1;
