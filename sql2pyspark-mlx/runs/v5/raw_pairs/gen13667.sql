WITH matched_keys AS (
    SELECT dept_id, manager_id
    FROM employees
    INTERSECT
    SELECT dept_id, manager_id
    FROM employees
    WHERE MONTH(hire_date) IN (6, 8, 12)
),
verified AS (
    SELECT s.*
    FROM employees s
    INNER JOIN matched_keys k ON s.dept_id = k.dept_id AND s.manager_id = k.manager_id
)
SELECT
    job_title,
    SUM(CASE WHEN job_title = 'Designer' THEN salary ELSE 0 END) AS designer_salary,
    PERCENTILE_APPROX(salary, 0.9) AS salary_p90,
    ROUND(AVG(salary), 2) AS avg_salary,
    ROUND(AVG(salary), 2) AS avg_salary_2
FROM verified
GROUP BY 1
HAVING SUM(CASE WHEN job_title = 'Designer' THEN salary ELSE 0 END) < 1000000;
