WITH base AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(location) AS location_upper
    FROM departments
    WHERE location = 'Bangkok'
)
-- Running and moving aggregates
SELECT
    id,
    location,
    location_upper,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
    ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share_2
FROM base;
