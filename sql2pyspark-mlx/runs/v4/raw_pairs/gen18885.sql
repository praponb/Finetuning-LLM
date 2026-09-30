WITH latest AS (
    SELECT *
    FROM employees
    QUALIFY RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) = 1
),
with_dims AS (
    SELECT
        a.*,
        b.hire_date AS employee_hire_date,
        b.level AS employee_level,
        b.email AS employee_email
    FROM latest a
    INNER JOIN employees b
        ON a.manager_id = b.id
),
with_windows AS (
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
        employee_hire_date,
        employee_level,
        employee_email,
        FIRST_VALUE(job_title, TRUE) OVER (ORDER BY employee_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title,
        SUM(salary) OVER (PARTITION BY job_title, dept_id ORDER BY employee_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM with_dims
),
joined AS (
    SELECT a.*, d.dept_name
    FROM with_windows a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
enriched AS (
    SELECT
        a.*,
        d.job_title AS employee_job_title,
        d.level AS employee_level_2
    FROM joined a
    INNER JOIN employees d
        ON a.manager_id = d.id
),
base AS (
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
        employee_hire_date,
        employee_level,
        employee_email,
        first_known_job_title,
        running_salary,
        dept_name,
        employee_job_title,
        employee_level_2,
        DATE_ADD(hire_date, 46) AS hire_date_plus_46d
    FROM enriched
    WHERE manager_id IS NOT NULL
),
by_type AS (
    SELECT *
    FROM (
        SELECT
            job_title,
            employee_level_2,
            running_salary
        FROM base
    ) src
    PIVOT (
        SUM(running_salary)
        FOR employee_level_2 IN ('L1', 'L5', 'L4')
    )
)
SELECT
    job_title,
    L5,
    L4,
    CASE WHEN job_title RLIKE '^(Designer|Analyst)$' THEN 'GROUP_A' WHEN job_title RLIKE '^(Engineer|Manager)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS job_title_zone
FROM by_type
ORDER BY L5
