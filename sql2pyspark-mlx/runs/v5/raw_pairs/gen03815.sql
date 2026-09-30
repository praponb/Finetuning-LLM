WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN level = 'L2' THEN 1 ELSE 0 END AS is_l2
    FROM employees
    WHERE level IS DISTINCT FROM 'L1'
      AND hire_date > DATE '2023-05-24'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_id,
        job_title,
        hire_date,
        is_remote,
        level,
        is_l2,
        CONCAT(job_title, ' - ', level) AS job_title_level,
        CASE WHEN is_l2 >= 61 THEN 'High' WHEN is_l2 >= 39 THEN 'Medium' ELSE 'Low' END AS is_l2_level
    FROM base
    WHERE manager_id IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(is_l2 * 1.07, 2) AS is_l2_adjusted,
        ADD_MONTHS(hire_date, 3) AS hire_date_plus_months
    FROM valid
    WHERE hire_date > DATE '2023-05-11'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id, job_title ORDER BY is_l2_adjusted DESC NULLS FIRST, id) AS latest_rank
        FROM cleaned
    ) x
    WHERE latest_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(is_l2_adjusted) OVER (PARTITION BY dept_id ORDER BY hire_date_plus_months, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_is_l2_adjusted,
        SUM(is_l2_adjusted) OVER (PARTITION BY dept_id ORDER BY hire_date_plus_months, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_is_l2_adjusted,
        MAX(is_l2_adjusted) OVER (PARTITION BY dept_id ORDER BY hire_date_plus_months, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_is_l2_adjusted
    FROM latest
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        e.*,
        d.location,
        d.budget
    FROM scored e
    LEFT JOIN departments d
        ON e.dept_id = d.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        dept_id,
        last7_sum_is_l2_adjusted,
        is_l2_adjusted,
        budget
    FROM with_dims
) src
UNPIVOT INCLUDE NULLS (
    reading FOR kpi IN (last7_sum_is_l2_adjusted AS LAST7_SUM_IS_L2_ADJUSTED_VAL, is_l2_adjusted, budget AS BUDGET_VAL)
);
