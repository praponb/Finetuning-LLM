WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date BETWEEN '2024-05-23' AND '2025-10-01'
        )
)
-- Grouped metrics
SELECT
    CASE WHEN budget >= 3989000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    location,
    ROUND(AVG(budget), 2) AS avg_budget,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    STDDEV_SAMP(budget) AS budget_stddev,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM eligible
GROUP BY budget_tier, location
HAVING ROUND(AVG(budget), 2) >= 0
ORDER BY num_rows ASC NULLS FIRST
