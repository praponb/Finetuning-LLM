WITH matched_keys AS (
    SELECT job_title, level
    FROM employees
    EXCEPT ALL
    SELECT job_title, level
    FROM employees
    WHERE email IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM employees s
    JOIN matched_keys k ON s.job_title = k.job_title AND s.level = k.level
)
SELECT
    manager_id,
    ROUND(AVG(salary), 2) AS avg_salary,
    AVG(salary) AS avg_salary_2,
    COUNT(DISTINCT manager_id) AS distinct_manager_ids
FROM verified
GROUP BY 1
HAVING AVG(salary) > 0
ORDER BY avg_salary_2 DESC
