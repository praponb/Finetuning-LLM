WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(dept_name, ' ', location) AS dept_name_location,
        ROUND(budget * 1.07, 2) AS budget_adjusted,
        COALESCE(location, 'NONE') AS location_clean
    FROM departments
    WHERE location IS NOT NULL
      AND dept_name IN ('Engineering', 'Finance', 'Marketing')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id ASC) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
ranked_departments AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        dept_name,
        budget_adjusted,
        location_clean
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_adjusted ASC NULLS FIRST, id ASC) AS row_num
        FROM ranked
    ) x
    WHERE row_num = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        budget_adjusted,
        location_clean,
        AVG(budget_adjusted) OVER (PARTITION BY dept_name ORDER BY budget_adjusted ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_adjusted_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_adjusted DESC, id ASC) AS row_seq,
        AVG(budget_adjusted) OVER (PARTITION BY dept_name) AS dept_name_avg_budget_adjusted,
        AVG(budget_adjusted) OVER (ORDER BY budget_adjusted ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_adjusted_avg_2,
        AVG(budget_adjusted) OVER (PARTITION BY dept_name ORDER BY budget_adjusted ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS budget_adjusted_avg_3_prior
    FROM ranked_departments
    WHERE dept_name IN ('HR', 'Engineering')
)
SELECT
    id,
    budget_adjusted,
    location_clean,
    row_seq,
    dept_name_avg_budget_adjusted
FROM with_windows
