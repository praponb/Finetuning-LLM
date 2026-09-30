WITH latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.employee_hire_date,
        top1.job_title
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    dept_name,
    CASE WHEN budget >= 3715000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    AVG(budget) AS avg_budget,
    COUNT(DISTINCT job_title) AS distinct_job_titles,
    ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN location = 'Tokyo' THEN 1 END), 0), 2) AS budget_per_tokyo
FROM latest_match
GROUP BY 1, 2
ORDER BY distinct_job_titles NULLS FIRST
