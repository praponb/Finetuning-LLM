WITH kept_keys AS (
    SELECT job_title, manager_id
    FROM employees
    EXCEPT DISTINCT
    SELECT job_title, manager_id
    FROM employees
    WHERE salary >= 124000
),
kept_rows AS (
    SELECT s.*
    FROM employees s
    INNER JOIN kept_keys k ON s.job_title = k.job_title AND s.manager_id = k.manager_id
)
SELECT
    CASE WHEN salary >= 131000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    SUM(CASE WHEN job_title = 'Engineer' THEN salary ELSE 0 END) AS engineer_salary,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(CASE WHEN level = 'L3' THEN 1 END) AS l3_count
FROM kept_rows
GROUP BY 1
HAVING engineer_salary >= 0
ORDER BY engineer_salary NULLS FIRST;
