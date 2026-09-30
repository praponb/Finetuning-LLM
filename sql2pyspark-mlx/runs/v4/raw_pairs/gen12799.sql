WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(budget, 1) AS budget_rounded,
        ROUND(budget * 0.85, 2) AS budget_adjusted,
        COALESCE(dept_name, 'N/A') AS dept_name_clean
    FROM departments
    WHERE location IS NOT NULL
)
-- Running and moving aggregates
SELECT
    *,
    RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS budget_rank,
    AVG(budget) OVER w_hist AS budget_avg_6,
    NTILE(4) OVER w_grp AS budget_rounded_bucket
FROM filtered
WINDOW w_hist AS (PARTITION BY location ORDER BY budget_adjusted, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY dept_name ORDER BY budget_rounded, id);
