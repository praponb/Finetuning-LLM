WITH valid AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        LAST_DAY(hire_date) AS hire_date_month_end,
        COALESCE(level, 'N/A') AS level_clean
    FROM employees
    WHERE hire_date > DATE '2023-05-10'
),
valid_employees AS (
    SELECT *, CASE WHEN level IN ('L3', 'L2') THEN 'Priority' ELSE 'Other' END AS level_group
    FROM valid
    WHERE hire_date > DATE '2023-07-07'
      AND level IS NOT NULL
),
with_dims AS (
    SELECT
        src.id,
        src.full_name,
        src.email,
        src.manager_id,
        src.job_title,
        src.salary,
        src.hire_date_month_end,
        src.level_clean,
        b.email AS employee_email
    FROM valid_employees src
    INNER JOIN employees b
        ON src.manager_id = b.id
    WHERE b.is_remote = TRUE
),
joined AS (
    SELECT
        a.*,
        p.full_name AS employee_full_name,
        p.email AS employee_email_2,
        p.hire_date
    FROM with_dims a
    LEFT JOIN employees p
        ON a.manager_id = p.id
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY salary DESC, id) AS rn
    FROM joined
    QUALIFY rn = 1
),
enriched AS (
    SELECT
        s.*,
        p.salary AS employee_salary,
        p.level
    FROM ranked s
    INNER JOIN employees p
        ON s.manager_id <=> p.id
),
joined_employees AS (
    SELECT
        a.id,
        a.full_name,
        a.email,
        a.manager_id,
        a.salary,
        a.level_clean,
        a.employee_email,
        a.employee_full_name,
        a.employee_email_2,
        a.rn,
        a.employee_salary,
        a.level,
        e.hire_date AS employee_hire_date,
        e.job_title AS employee_job_title,
        e.full_name AS employee_full_name_2
    FROM enriched a
    LEFT JOIN employees e
        ON a.manager_id <=> e.id
),
totals AS (
    SELECT
        employee_job_title,
        SUM(employee_salary) FILTER (WHERE employee_job_title = 'Designer') AS designer_total_employee_salary,
        COUNT(*) FILTER (WHERE level = 'L2') AS l2_rows
    FROM joined_employees
    GROUP BY employee_job_title
)
SELECT
    employee_job_title,
    designer_total_employee_salary,
    l2_rows,
    CASE WHEN employee_job_title RLIKE '^(Analyst|Designer)$' THEN 'GROUP_A' WHEN employee_job_title RLIKE '^(Engineer|Manager)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS employee_job_title_zone
FROM totals
ORDER BY employee_job_title_zone, l2_rows DESC;
