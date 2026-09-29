WITH cleaned AS (
    SELECT
        *,
        ROUND(budget, 1) AS budget_rounded,
        COALESCE(location, 'UNKNOWN') AS location_clean
    FROM departments
    WHERE location = 'Berlin'
)
SELECT
    *,
    SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget,
    CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget NULLS LAST) AS budget_cume_dist,
    SUM(budget_rounded) OVER (PARTITION BY dept_name ORDER BY budget_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_rounded,
    AVG(budget) OVER (ORDER BY budget_rounded, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_avg_5_prior
FROM cleaned;
