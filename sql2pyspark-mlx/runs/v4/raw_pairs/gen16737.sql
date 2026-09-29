WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END is_manager,
        COALESCE(job_title, 'DIRECT') resolved_job_title
    FROM employees
    WHERE hire_date > DATE '2023-06-16'
      AND job_title IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN job_title IN ('Manager', 'Director') THEN 'Priority' ELSE 'Other' END job_title_group
    FROM base
    WHERE hire_date >= TO_DATE('2023-03-29')
      AND level IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT level, manager_id
    FROM filtered
    EXCEPT ALL
    SELECT level, manager_id
    FROM employees
    WHERE LOWER(TRIM(email)) = 'alice@gmail.com'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.level = k.level AND s.manager_id = k.manager_id
),
filtered_employees AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(is_remote AS INT) is_remote_flag
    FROM verified
    WHERE hire_date > DATE '2023-12-22'
      AND job_title IS NOT NULL
      AND is_manager > 39
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.is_remote,
    s.level,
    s.is_manager,
    CASE WHEN job_title RLIKE '^(Designer|Director)$' THEN 'EXTENDED' WHEN job_title RLIKE '^(Engineer|Analyst)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS job_title_zone,
    (SELECT MIN(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS min_employee_salary
FROM filtered_employees s
ORDER BY level, manager_id;
