WITH valid AS (
    SELECT *, COALESCE(job_title, 'Unknown') AS job_title_filled
    FROM employees
    WHERE job_title IS NOT NULL
      AND LOWER(TRIM(full_name)) = 'emma brown'
),
enriched AS (
    SELECT
        a.*,
        e.job_title AS employee_job_title,
        e.hire_date AS employee_hire_date
    FROM valid a
    LEFT JOIN employees e
        ON a.manager_id = e.id
),
with_dims AS (
    SELECT
        src.*,
        p.level AS employee_level,
        p.hire_date AS employee_hire_date_2
    FROM enriched src
    JOIN employees p
        ON src.manager_id = p.id
),
with_dims_employees AS (
    SELECT
        e.id,
        e.full_name,
        e.dept_id,
        e.manager_id,
        e.job_title,
        e.salary,
        e.is_remote,
        e.level,
        e.employee_hire_date,
        p2.salary AS employee_salary,
        p2.hire_date AS employee_hire_date_3,
        p2.level AS employee_level_2
    FROM with_dims e
    LEFT JOIN employees p2
        ON e.manager_id = p2.id
),
valid_employees AS (
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        employee_hire_date,
        employee_salary,
        employee_level_2,
        employee_salary - salary AS employee_salary_minus_salary,
        COALESCE(level, 'N/A') AS resolved_level
    FROM with_dims_employees
    WHERE manager_id IS NOT NULL
)
SELECT
    CASE WHEN employee_salary >= 133000 THEN 'LARGE' ELSE 'SMALL' END AS employee_salary_tier,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(*) FILTER (WHERE level = 'L3') AS l3_rows,
    COUNT(*) FILTER (WHERE level = 'L3') AS l3_rows_2
FROM valid_employees
GROUP BY CASE WHEN employee_salary >= 133000 THEN 'LARGE' ELSE 'SMALL' END
