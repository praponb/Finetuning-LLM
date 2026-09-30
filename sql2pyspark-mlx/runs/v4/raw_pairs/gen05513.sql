WITH base AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(dept_name)) AS dept_name_title
    FROM departments
    WHERE budget <= 2389000
)
-- Window metrics over several different partitions
SELECT
    id,
    dept_name,
    location,
    budget,
    dept_name_title,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share_2
FROM base;
