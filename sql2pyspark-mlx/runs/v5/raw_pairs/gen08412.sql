WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(budget AS INT) AS budget_int,
        CONCAT(dept_name, ' - ', location) AS dept_name_location,
        COALESCE(dept_name, 'UNKNOWN') AS resolved_dept_name
    FROM departments
    WHERE location IS NOT NULL
      AND location = 'Bangkok'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_int,
        LAST_VALUE(dept_name, TRUE) OVER (ORDER BY budget_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_seq,
        DENSE_RANK() OVER (ORDER BY budget NULLS FIRST) AS budget_dense_rank
    FROM base
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.row_seq,
        s.budget_dense_rank,
        x.employee_level,
        x.salary
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.level AS employee_level, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.row_seq,
        s.employee_level,
        top1.employee_salary
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(row_seq * 1.07, 2) AS row_seq_scaled,
        row_seq - employee_salary AS row_seq_minus_employee_salary,
        COALESCE(employee_level, 'OTHER') AS employee_level_clean
    FROM with_last
    WHERE employee_level IS NOT NULL
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.employee_level_2,
        x.employee_job_title
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level_2, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    row_seq,
    employee_salary,
    row_seq_minus_employee_salary,
    employee_level_clean,
    employee_job_title
FROM latest_match;
