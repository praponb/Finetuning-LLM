WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN level = 'L5' THEN 1 ELSE 0 END AS is_l5,
        COALESCE(job_title, 'UNKNOWN') AS job_title_clean
    FROM employees
    WHERE job_title IS NOT DISTINCT FROM 'Manager'
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        s.id,
        s.full_name,
        s.manager_id,
        s.hire_date,
        s.is_remote,
        d.email AS employee_email,
        d.job_title AS employee_job_title
    FROM base s
    INNER JOIN employees d
        ON s.manager_id = d.id
    WHERE d.dept_id IN (17, 23, 24, 37)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, manager_id
    FROM with_dims
    EXCEPT DISTINCT
    SELECT id, manager_id
    FROM employees
    WHERE full_name IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.id = k.id AND s.manager_id = k.manager_id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        manager_id,
        hire_date,
        is_remote,
        employee_email,
        employee_job_title,
        LAST_DAY(hire_date) AS hire_date_month_end
    FROM reconciled
    WHERE hire_date > TO_DATE('2023-05-25')
      AND employee_job_title IS NOT NULL
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', hire_date_month_end) AS week_start,
    SORT_ARRAY(COLLECT_SET(employee_job_title)) AS employee_job_title_list,
    SORT_ARRAY(COLLECT_SET(employee_job_title)) AS employee_job_title_list_2,
    COUNT(DISTINCT manager_id) AS distinct_manager_ids,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE employee_job_title = 'Designer') AS designer_rows
FROM filtered
GROUP BY DATE_TRUNC('WEEK', hire_date_month_end)
ORDER BY distinct_manager_ids DESC NULLS LAST;
