WITH base AS (
    SELECT *, TRIM(location) clean_location
    FROM departments
    WHERE location IN ('London', 'Chiang Mai')
),
agg AS (
    SELECT dept_name, SUM(budget) total_budget
    FROM base
    GROUP BY dept_name
)
SELECT dept_name, total_budget
FROM agg
WHERE total_budget >= 11358000;
