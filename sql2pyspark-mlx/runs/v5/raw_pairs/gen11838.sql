WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.job_title IN ('Designer', 'Director')
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN budget >= 3053000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    dept_name,
    ROUND(AVG(budget), 2) AS avg_budget,
    COUNT_IF(location = 'Chiang Mai') AS chiang_mai_events,
    SUM(budget) FILTER (WHERE location = 'Singapore') AS singapore_total_budget,
    RANK() OVER (PARTITION BY CASE WHEN budget >= 3053000 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SUM(budget) FILTER (WHERE location = 'Singapore') DESC) AS singapore_total_budget_rank
FROM eligible
GROUP BY CASE WHEN budget >= 3053000 THEN 'LARGE' ELSE 'SMALL' END, dept_name
HAVING COUNT_IF(location = 'Chiang Mai') >= 1
ORDER BY budget_tier NULLS FIRST
