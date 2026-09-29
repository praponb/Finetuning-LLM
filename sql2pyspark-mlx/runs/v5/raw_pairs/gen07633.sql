WITH base AS (
    SELECT
        *,
        LENGTH(job_title) AS job_title_length,
        COALESCE(job_title, 'NONE') AS resolved_job_title
    FROM employees
    WHERE hire_date >= DATE '2023-03-28'
),
enriched AS (
    SELECT
        s.*,
        b.hire_date AS employee_hire_date,
        b.job_title AS employee_job_title,
        b.level AS employee_level
    FROM base s
    LEFT JOIN employees b
        ON s.manager_id = b.id
    WHERE b.is_remote = TRUE
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY job_title_length DESC NULLS LAST, id ASC) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
),
joined AS (
    SELECT
        src.*,
        d.full_name AS employee_full_name,
        d.email AS employee_email
    FROM ranked src
    LEFT JOIN employees d
        ON src.manager_id = d.id
),
with_dims AS (
    SELECT
        a.*,
        d.dept_name,
        d.budget,
        d.location
    FROM joined a
    LEFT JOIN departments d
        ON a.dept_id = d.id
)
SELECT
    p.job_title AS employee_job_title,
    e.employee_job_title AS employee_employee_job_title,
    COUNT(*) AS rows_total,
    SUM(e.salary) AS total_salary,
    SUM(e.budget) AS total_budget,
    GROUPING_ID() AS gid
FROM with_dims e
INNER JOIN employees p ON e.manager_id = p.id
GROUP BY GROUPING SETS ((p.job_title, e.employee_job_title), (p.job_title))
