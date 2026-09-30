WITH with_latest AS (
    SELECT s.*, latest.employee_salary
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    dept_name,
    SUM(CASE WHEN dept_name = 'Engineering' THEN budget ELSE 0 END) AS engineering_budget,
    COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END) AS engineering_count,
    COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
FROM with_latest
GROUP BY 1
ORDER BY chiang_mai_count NULLS FIRST;
