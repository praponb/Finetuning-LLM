WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, level
    FROM employees
    EXCEPT
    SELECT id, level
    FROM employees
    WHERE email IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN kept_keys k ON s.id = k.id AND s.level = k.level
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(level) AS clean_level,
        IF(is_remote, 'Yes', 'No') AS is_remote_label,
        COALESCE(job_title, 'OTHER') AS job_title_filled
    FROM verified
    WHERE hire_date >= DATE '2023-08-22'
      AND job_title IS DISTINCT FROM 'Designer'
),
joined AS (
    -- Attach employees attributes
    SELECT
        src.id,
        src.full_name,
        src.email,
        src.dept_id,
        src.manager_id,
        src.job_title,
        src.salary,
        src.hire_date,
        src.is_remote,
        src.level,
        src.clean_level,
        src.is_remote_label,
        src.job_title_filled,
        p.email AS employee_email,
        p.salary AS employee_salary
    FROM base src
    INNER JOIN employees p
        ON src.manager_id <=> p.id
),
joined_employees AS (
    -- Attach employees attributes
    SELECT
        e.*,
        b.email AS employee_email_2,
        b.level AS employee_level
    FROM joined e
    LEFT JOIN employees b
        ON e.manager_id = b.id
    WHERE b.dept_id IN (12, 17, 18)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        is_remote,
        level,
        is_remote_label,
        employee_email,
        employee_salary,
        employee_email_2,
        UPPER(TRIM(employee_email)) AS employee_email_normalized
    FROM joined_employees
    WHERE LENGTH(employee_email_2) < 9
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(full_name, 1, 2) AS full_name_prefix
    FROM filtered
    WHERE job_title IS NOT NULL
      AND level IS NOT DISTINCT FROM 'L3'
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.full_name,
        a.email,
        a.dept_id,
        a.is_remote_label,
        a.employee_email,
        a.employee_salary,
        a.full_name_prefix,
        b.location,
        b.dept_name,
        b.budget
    FROM valid a
    INNER JOIN departments b
        ON a.dept_id = b.id
)
-- Grouped metrics
SELECT
    CASE WHEN employee_salary >= 131000 THEN 'HIGH' ELSE 'LOW' END AS employee_salary_tier,
    dept_id,
    COUNT_IF(dept_name = 'Legal') AS legal_events,
    COUNT(DISTINCT dept_id) AS distinct_dept_ids,
    COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END) AS marketing_count,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names
FROM with_dims
GROUP BY CASE WHEN employee_salary >= 131000 THEN 'HIGH' ELSE 'LOW' END, dept_id
ORDER BY dept_id DESC NULLS FIRST
