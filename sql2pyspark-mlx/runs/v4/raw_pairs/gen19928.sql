WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        dept_id,
        job_title,
        hire_date,
        level,
        INITCAP(TRIM(full_name)) AS full_name_title
    FROM employees
    WHERE hire_date > DATE '2023-10-02'
      AND job_title IS NOT NULL
),
with_dims AS (
    -- Attach departments attributes
    SELECT s.*, p.budget
    FROM cleaned s
    LEFT JOIN departments p
        ON s.dept_id = p.id
),
with_dims_employees AS (
    -- Attach departments attributes
    SELECT
        a.*,
        b.dept_name,
        b.location,
        b.budget AS department_budget
    FROM with_dims a
    LEFT JOIN departments b
        ON a.dept_id = b.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) = 1
),
enriched AS (
    -- Attach departments attributes
    SELECT
        s.*,
        p.budget AS department_budget_2,
        p.dept_name AS department_dept_name
    FROM ranked s
    LEFT JOIN departments p
        ON s.dept_id = p.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(department_budget) OVER (PARTITION BY level, dept_name ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_department_budget,
        MAX(department_budget) OVER (PARTITION BY job_title) AS job_title_max_department_budget,
        AVG(department_budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS department_budget_avg_2
    FROM enriched
),
with_dims_employees_2 AS (
    -- Attach departments attributes
    SELECT
        src.*,
        d.dept_name AS department_dept_name_2,
        d.budget AS department_budget_3
    FROM scored src
    INNER JOIN departments d
        ON src.dept_id = d.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN department_budget_2 >= 3136000 THEN 'HIGH' ELSE 'LOW' END AS department_budget_2_tier,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    MIN(budget) AS min_budget,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM with_dims_employees_2
GROUP BY department_budget_2_tier
HAVING num_dept_names > 1
ORDER BY department_budget_2_tier DESC
