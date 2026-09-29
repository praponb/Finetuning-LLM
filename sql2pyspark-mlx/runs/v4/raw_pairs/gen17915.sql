WITH with_dims AS (
    -- Attach employees attributes
    SELECT
        s.*,
        e.job_title AS employee_job_title,
        e.email AS employee_email
    FROM employees s
    LEFT JOIN employees e
        ON s.manager_id = e.id
),
joined AS (
    -- Attach employees attributes
    SELECT
        s.*,
        d.full_name AS employee_full_name,
        d.hire_date AS employee_hire_date
    FROM with_dims s
    JOIN employees d
        ON s.manager_id = d.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        employee_full_name,
        employee_hire_date,
        CONCAT_WS('/', job_title, full_name) AS job_title_full_name_key
    FROM joined
    WHERE hire_date > DATE '2023-06-16'
      AND employee_job_title IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY employee_hire_date DESC NULLS FIRST, id) AS rn
        FROM valid
    ) t
    WHERE rn = 1
),
joined_employees AS (
    -- Attach departments attributes
    SELECT
        a.*,
        d.budget,
        d.location
    FROM deduped a
    LEFT JOIN departments d
        ON a.dept_id = d.id
)
SELECT
    id,
    manager_id,
    hire_date,
    is_remote,
    employee_full_name,
    job_title_full_name_key,
    rn,
    budget,
    location
FROM joined_employees
