WITH base AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(location) AS location_upper
    FROM departments
    WHERE dept_name IS NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id IS NOT NULL
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        budget,
        location_upper,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(dept_name, 'UNKNOWN') AS resolved_dept_name
    FROM eligible
    WHERE location IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS LAST, id ASC) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        budget,
        location_upper,
        budget_filled,
        resolved_dept_name,
        NTILE(5) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC) AS budget_bucket,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget_filled DESC, id ASC) AS budget_filled_bucket,
        FIRST_VALUE(dept_name, TRUE) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name
    FROM deduped
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN budget_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS budget_bucket_tier,
    CASE WHEN budget >= 4488000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    dept_name,
    ROUND(AVG(budget), 2) AS avg_budget,
    ROUND(SUM(budget_filled) / NULLIF(COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END), 0), 2) AS budget_filled_per_engineering
FROM metrics
GROUP BY budget_bucket_tier, budget_tier, dept_name
ORDER BY budget_filled_per_engineering DESC;
