WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(location)) AS location_title
    FROM departments
    WHERE dept_name IS NOT NULL
      AND budget IS NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    dept_name,
    budget,
    location_title,
    ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
    NTILE(4) OVER (PARTITION BY location ORDER BY budget DESC, id) AS budget_bucket
FROM cleaned
