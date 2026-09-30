WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(full_name) AS full_name_lower,
        COALESCE(job_title, 'N/A') AS resolved_job_title
    FROM employees
    WHERE level IS NOT NULL
      AND is_remote
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        e.id,
        e.dept_id,
        e.job_title,
        e.salary,
        e.level,
        e.full_name_lower,
        e.resolved_job_title,
        p.budget,
        p.location,
        p.dept_name
    FROM valid e
    LEFT JOIN departments p
        ON e.dept_id = p.id
    WHERE p.dept_name = 'Sales'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT job_title, level
    FROM with_dims
    INTERSECT
    SELECT job_title, level
    FROM employees
    WHERE full_name LIKE 'J%'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN remaining_keys k ON s.job_title = k.job_title AND s.level = k.level
),
with_dims_employees AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.dept_id,
        src.salary,
        src.level,
        src.full_name_lower,
        src.resolved_job_title,
        src.budget,
        src.location,
        src.dept_name,
        b.budget AS department_budget,
        b.dept_name AS department_dept_name
    FROM kept_rows src
    LEFT JOIN departments b
        ON src.dept_id = b.id
),
with_dims_employees_2 AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.dept_id,
        src.salary,
        src.level,
        src.department_budget,
        src.department_dept_name,
        b.budget AS department_budget_2,
        b.dept_name AS department_dept_name_2
    FROM with_dims_employees src
    LEFT JOIN departments b
        ON src.dept_id = b.id
)
-- Join a dimension and aggregate
SELECT
    dim.location,
    src.department_dept_name_2,
    COUNT(*) AS num_rows,
    ROUND(AVG(src.department_budget_2), 2) AS avg_department_budget_2,
    GROUPING_ID() AS gid
FROM with_dims_employees_2 src
LEFT JOIN departments dim ON src.dept_id = dim.id
GROUP BY CUBE (dim.location, src.department_dept_name_2);
