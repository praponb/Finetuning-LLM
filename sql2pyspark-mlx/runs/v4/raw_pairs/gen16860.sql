WITH enriched AS (
    -- Attach employees attributes
    SELECT
        e.*,
        d.job_title AS employee_job_title,
        d.salary AS employee_salary,
        d.hire_date AS employee_hire_date
    FROM employees e
    LEFT JOIN employees d
        ON e.manager_id = d.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY employee_hire_date DESC NULLS LAST, id ASC) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_job_title, dept_id ORDER BY employee_salary DESC NULLS FIRST, id ASC) AS latest_rank
        FROM ranked
    ) x
    WHERE latest_rank = 1
),
enriched_employees AS (
    -- Attach departments attributes
    SELECT
        e.*,
        p.location,
        p.dept_name,
        p.budget
    FROM deduped e
    INNER JOIN departments p
        ON e.dept_id = p.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title, employee_job_title ORDER BY salary DESC, id ASC) = 1
),
ranked_employees AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY dept_id ORDER BY salary ASC) = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(email, ', ', level) AS email_level,
        COALESCE(dept_name, 'N/A') AS resolved_dept_name
    FROM ranked_employees
    WHERE manager_id IS NOT NULL
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    (SELECT SUM(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS sum_employee_salary
FROM filtered s;
