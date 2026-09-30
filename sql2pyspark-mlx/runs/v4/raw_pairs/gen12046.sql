WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(dept_name) dept_name_lower
    FROM departments
    WHERE budget BETWEEN 2065000 AND 3056000
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) budget_avg_2,
        DENSE_RANK() OVER (PARTITION BY dept_name, location ORDER BY budget) budget_dense_rank,
        AVG(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) budget_avg_6_prior
    FROM cleaned
),
kept_keys AS (
    -- Set difference on keys
    SELECT dept_name
    FROM metrics
    EXCEPT DISTINCT
    SELECT dept_name
    FROM departments
    WHERE dept_name = 'Finance'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.dept_name = k.dept_name
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            dept_name,
            location,
            budget_avg_6_prior
        FROM reconciled
    ) src
    PIVOT (
        MAX(budget_avg_6_prior) AS total,
        COUNT(1) AS item_count
        FOR location IN ('Singapore' AS singapore, 'Chiang Mai' AS chiang_mai)
    )
)
SELECT
    dept_name,
    singapore_total,
    singapore_item_count,
    chiang_mai_total,
    chiang_mai_item_count
FROM by_type
ORDER BY singapore_total;
