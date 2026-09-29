WITH with_windows AS (
    SELECT
        *,
        MIN(budget) OVER (PARTITION BY location) AS location_min_budget,
        AVG(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS budget_avg_6_prior,
        NTILE(3) OVER w_running AS budget_bucket
    FROM departments
    WINDOW w_running AS (PARTITION BY location ORDER BY budget DESC, id)
)
SELECT
    id,
    location,
    budget_avg_6_prior,
    budget_bucket
FROM with_windows
ORDER BY budget_bucket NULLS FIRST
