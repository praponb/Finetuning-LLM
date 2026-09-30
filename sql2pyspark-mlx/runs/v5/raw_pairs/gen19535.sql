WITH cleaned AS (
    SELECT *, ROUND(budget, 1) budget_rounded
    FROM departments
    WHERE location IS NOT NULL
)
SELECT
    id,
    location,
    budget,
    budget_rounded,
    PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget_rounded DESC NULLS FIRST) budget_rounded_percent_rank,
    SUM(budget_rounded) OVER w_running cumulative_budget_rounded,
    NTILE(5) OVER (ORDER BY budget_rounded DESC, id) budget_rounded_bucket
FROM cleaned
WHERE location IN ('Singapore', 'Chiang Mai', 'New York')
WINDOW w_running AS (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
