WITH kept_keys AS (
    SELECT manager_id, id
    FROM employees
    EXCEPT ALL
    SELECT manager_id, id
    FROM employees
    WHERE salary BETWEEN 64000 AND 78000
),
reconciled AS (
    SELECT s.*
    FROM employees s
    INNER JOIN kept_keys k ON s.manager_id = k.manager_id AND s.id = k.id
)
SELECT
    CASE WHEN salary >= 146000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    dept_id,
    COUNT(DISTINCT level) AS distinct_levels,
    ROUND(AVG(salary), 2) AS avg_salary,
    SUM(salary) AS total_salary,
    SUM(CASE WHEN level = 'L1' THEN salary ELSE 0 END) AS l1_salary
FROM reconciled
GROUP BY ROLLUP (CASE WHEN salary >= 146000 THEN 'PREMIUM' ELSE 'STANDARD' END, dept_id)
ORDER BY avg_salary DESC NULLS LAST
