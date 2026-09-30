WITH with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.employee_level,
        x.salary
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN salary >= 134000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    CASE WHEN budget >= 3885000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    MAX(salary) FILTER (WHERE employee_level = 'L1') AS l1_max_salary,
    COUNT(CASE WHEN employee_level = 'L1' THEN 1 END) AS l1_count,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN location = 'Bangkok' THEN 1 END), 0), 2) AS salary_per_bangkok,
    APPROX_PERCENTILE(salary, 0.95) AS salary_p95,
    APPROX_PERCENTILE(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    COUNT(CASE WHEN location = 'London' THEN 1 END) AS london_count,
    GROUPING(CASE WHEN salary >= 134000 THEN 'LARGE' ELSE 'SMALL' END) AS gid
FROM with_last
GROUP BY CUBE (CASE WHEN salary >= 134000 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN budget >= 3885000 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY l1_count DESC NULLS FIRST;
