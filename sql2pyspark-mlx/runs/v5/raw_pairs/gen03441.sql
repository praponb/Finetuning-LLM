WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS dedup_rank
    FROM departments
    QUALIFY dedup_rank = 1
)
SELECT
    CASE WHEN budget >= 3357000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    MIN(budget) AS min_budget,
    APPROX_PERCENTILE(budget, 0.95) AS budget_p95,
    COUNT(DISTINCT location) AS distinct_locations,
    COUNT(*) AS event_count
FROM ranked
GROUP BY CASE WHEN budget >= 3357000 THEN 'HIGH' ELSE 'LOW' END
HAVING MIN(budget) > 0
ORDER BY min_budget DESC
