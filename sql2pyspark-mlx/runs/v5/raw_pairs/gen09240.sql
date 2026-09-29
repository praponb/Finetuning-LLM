WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        DATE_FORMAT(hire_date, 'MMM yyyy') AS hire_date_label,
        CONCAT_WS('-', full_name, job_title) AS full_name_job_title_key,
        COALESCE(level, 'DIRECT') AS resolved_level
    FROM employees
    WHERE is_remote = TRUE
),
enriched AS (
    -- Attach employees attributes
    SELECT e.*, d.level AS employee_level
    FROM valid e
    INNER JOIN employees d
        ON e.manager_id <=> d.id
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.email,
        a.manager_id,
        a.job_title,
        a.hire_date,
        a.is_remote,
        a.hire_date_label,
        a.full_name_job_title_key,
        a.resolved_level,
        b.hire_date AS employee_hire_date
    FROM enriched a
    INNER JOIN employees b
        ON a.manager_id = b.id
    WHERE b.dept_id IN (3, 19)
),
enriched_employees AS (
    -- Attach employees attributes
    SELECT
        s.*,
        d.level,
        d.salary,
        d.email AS employee_email
    FROM joined s
    LEFT JOIN employees d
        ON s.manager_id <=> d.id
),
valid_employees AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(level, 'OTHER') AS resolved_level_2
    FROM enriched_employees
    WHERE hire_date >= DATE '2023-02-28'
)
-- Join a dimension and aggregate
SELECT
    d.job_title AS employee_job_title,
    e.job_title AS employee_job_title,
    COUNT(*) AS num_rows,
    MIN(e.is_remote_flag) AS min_is_remote_flag,
    SUM(e.salary) AS total_salary
FROM valid_employees e
INNER JOIN employees d ON e.manager_id = d.id
GROUP BY CUBE (d.job_title, e.job_title);
