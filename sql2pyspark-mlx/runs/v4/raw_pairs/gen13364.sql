SELECT
    *,
    NTILE(5) OVER w_recent AS budget_bucket,
    AVG(budget) OVER (PARTITION BY location) AS location_avg_budget
FROM departments
WINDOW w_recent AS (ORDER BY budget, id)
