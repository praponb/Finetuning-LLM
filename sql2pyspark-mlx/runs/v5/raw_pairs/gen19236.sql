WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        location,
        budget,
        SUM(budget) OVER w_ordered AS running_sum_budget,
        MIN(budget) OVER w_ordered AS running_min_budget,
        COUNT(budget) OVER w_ordered AS running_count_budget,
        LAG(budget, 2) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) AS prev_budget
    FROM departments
    WINDOW w_ordered AS (PARTITION BY location ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    location,
    CASE WHEN budget >= 4378000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    PERCENTILE_APPROX(running_sum_budget, ARRAY(0.25, 0.5, 0.75)) AS running_sum_budget_quartiles,
    ROUND(AVG(running_min_budget), 2) AS avg_running_min_budget,
    SUM(CASE WHEN location = 'Bangkok' THEN running_sum_budget ELSE 0 END) AS bangkok_running_sum_budget,
    SUM(CASE WHEN location = 'New York' THEN running_sum_budget ELSE 0 END) AS new_york_running_sum_budget,
    SUM(running_sum_budget) AS total_running_sum_budget
FROM enriched
GROUP BY location, budget_tier
ORDER BY budget_tier ASC;
