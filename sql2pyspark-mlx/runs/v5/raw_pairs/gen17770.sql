WITH enriched AS (
    SELECT
        src.id,
        src.full_name,
        src.dept_id,
        src.manager_id,
        src.job_title,
        src.hire_date,
        src.is_remote,
        src.level,
        b.location,
        b.budget
    FROM employees src
    INNER JOIN departments b
        ON src.dept_id = b.id
),
enriched_employees AS (
    SELECT
        src.id,
        src.full_name,
        src.dept_id,
        src.manager_id,
        src.is_remote,
        src.level,
        src.location,
        d.dept_name
    FROM enriched src
    LEFT JOIN departments d
        ON src.dept_id = d.id
),
enriched_employees_2 AS (
    SELECT
        e.id,
        e.full_name,
        e.dept_id,
        e.manager_id,
        e.level,
        e.dept_name,
        d.location AS department_location,
        d.dept_name AS department_dept_name
    FROM enriched_employees e
    INNER JOIN departments d
        ON e.dept_id = d.id
),
with_dims AS (
    SELECT
        a.id,
        a.full_name,
        a.dept_id,
        a.level,
        a.dept_name,
        a.department_location,
        a.department_dept_name,
        d.dept_name AS department_dept_name_2,
        d.budget
    FROM enriched_employees_2 a
    INNER JOIN departments d
        ON a.dept_id = d.id
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY department_location ORDER BY budget DESC NULLS FIRST, id) AS rn
    FROM with_dims
    QUALIFY rn = 1
),
joined AS (
    SELECT
        e.*,
        d.dept_name AS department_dept_name_3,
        d.budget AS department_budget,
        d.location
    FROM ranked e
    INNER JOIN departments d
        ON e.dept_id = d.id
)
SELECT
    id,
    department_dept_name_2,
    metric,
    reading
FROM (
    SELECT
        id,
        department_dept_name_2,
        budget,
        department_budget
    FROM joined
) src
UNPIVOT (
    reading FOR metric IN (budget, department_budget)
);
