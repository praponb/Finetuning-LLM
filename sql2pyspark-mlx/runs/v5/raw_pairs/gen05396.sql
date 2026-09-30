WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END is_remote_label,
        CAST(salary AS INT) salary_int
    FROM employees
    WHERE hire_date >= DATE '2023-07-03'
      AND job_title IS DISTINCT FROM 'Designer'
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.*,
        p.hire_date AS employee_hire_date,
        p.salary AS employee_salary,
        p.email AS employee_email
    FROM cleaned a
    LEFT JOIN employees p
        ON a.manager_id = p.id
    WHERE p.hire_date <= '2024-09-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN level = 'L1' THEN 1 ELSE 0 END is_l1,
        COALESCE(level, 'UNKNOWN') level_clean
    FROM joined
    WHERE hire_date > DATE '2023-07-15'
      AND job_title IS NOT NULL
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.email,
        e.manager_id,
        e.job_title,
        e.employee_hire_date,
        e.employee_salary,
        d.level AS employee_level,
        d.hire_date AS employee_hire_date_2
    FROM filtered e
    INNER JOIN employees d
        ON e.manager_id <=> d.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        employee_level,
        CASE WHEN employee_salary >= 122000 THEN 'LARGE' ELSE 'SMALL' END AS employee_salary_tier,
        SUM(CASE WHEN employee_level = 'L2' THEN employee_salary ELSE 0 END) AS l2_employee_salary,
        SUM(CASE WHEN job_title = 'Director' THEN employee_salary ELSE 0 END) AS director_employee_salary
    FROM with_dims
    GROUP BY employee_level, employee_salary_tier
)
SELECT
    employee_level,
    employee_salary_tier,
    l2_employee_salary,
    director_employee_salary
FROM totals;
