WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rank_in_group
    FROM employees
    QUALIFY rank_in_group = 1
),
latest_employees AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC NULLS LAST, id) = 1
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        src.*,
        b.salary AS employee_salary,
        b.job_title AS employee_job_title,
        b.email
    FROM latest_employees src
    LEFT JOIN employees b
        ON src.manager_id = b.id
    WHERE MONTH(b.hire_date) IN (7, 8, 11)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('/', email, job_title) email_job_title_key,
        COALESCE(employee_job_title, 'N/A') employee_job_title_filled
    FROM with_dims
    WHERE email LIKE '%@startup.io'
),
enriched AS (
    -- Attach employees attributes
    SELECT
        src.id,
        src.manager_id,
        src.job_title,
        src.salary,
        src.hire_date,
        src.employee_salary,
        src.email,
        src.employee_job_title_filled,
        p.full_name,
        p.level,
        p.hire_date AS employee_hire_date
    FROM filtered src
    LEFT JOIN employees p
        ON src.manager_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        manager_id,
        employee_salary,
        employee_job_title_filled,
        full_name,
        level,
        ROW_NUMBER() OVER (PARTITION BY job_title, manager_id ORDER BY employee_salary DESC, id) row_seq,
        LAST_VALUE(employee_salary, TRUE) OVER (PARTITION BY manager_id ORDER BY employee_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_employee_salary,
        SUM(salary) OVER (PARTITION BY manager_id, level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_salary
    FROM enriched
),
totals AS (
    -- Multi-level aggregation
    SELECT
        manager_id,
        CASE WHEN last_known_employee_salary >= 125000 THEN 'HIGH' ELSE 'LOW' END AS last_known_employee_salary_tier,
        level,
        AVG(employee_salary) AS avg_employee_salary,
        SORT_ARRAY(COLLECT_SET(level)) AS level_list,
        ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN level = 'L5' THEN 1 END), 0), 2) AS row_seq_per_l5
    FROM with_windows
    GROUP BY manager_id, CASE WHEN last_known_employee_salary >= 125000 THEN 'HIGH' ELSE 'LOW' END, level
)
SELECT
    manager_id,
    last_known_employee_salary_tier,
    level,
    avg_employee_salary
FROM totals
