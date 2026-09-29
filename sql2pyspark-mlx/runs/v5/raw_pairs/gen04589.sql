WITH valid AS (
    SELECT
        *,
        REGEXP_REPLACE(email, ' ', '_') AS email_slug,
        COALESCE(level, 'NONE') AS level_filled
    FROM employees
    WHERE hire_date >= DATE '2023-03-12'
      AND level NOT IN ('L5', 'L4')
),
joined AS (
    SELECT
        s.id,
        s.full_name,
        s.email,
        s.dept_id,
        s.manager_id,
        s.job_title,
        s.salary,
        s.hire_date,
        s.email_slug,
        e.hire_date AS employee_hire_date,
        e.email AS employee_email
    FROM valid s
    LEFT JOIN employees e
        ON s.manager_id = e.id
    WHERE e.salary > 131000
),
remaining_keys AS (
    SELECT dept_id, manager_id
    FROM joined
    EXCEPT ALL
    SELECT dept_id, manager_id
    FROM employees
    WHERE full_name LIKE 'Oli%'
),
reconciled AS (
    SELECT s.*
    FROM joined s
    INNER JOIN remaining_keys k ON s.dept_id = k.dept_id AND s.manager_id = k.manager_id
),
with_dims AS (
    SELECT
        src.id,
        src.full_name,
        src.email,
        src.manager_id,
        src.job_title,
        src.email_slug,
        src.employee_hire_date,
        e.email AS employee_email_2,
        e.level
    FROM reconciled src
    LEFT JOIN employees e
        ON src.manager_id = e.id
),
base AS (
    SELECT
        *,
        LENGTH(full_name) AS full_name_length,
        COALESCE(job_title, 'N/A') AS job_title_clean
    FROM with_dims
    WHERE email IS NOT NULL
      AND job_title IS NOT NULL
)
SELECT
    d.level AS employee_level,
    s.job_title AS employee_job_title,
    COUNT(*) AS rows_total,
    MIN(s.full_name_length) AS min_full_name_length
FROM base s
INNER JOIN employees d ON s.manager_id = d.id
GROUP BY d.level, s.job_title
