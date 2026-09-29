WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        hire_date,
        is_remote,
        level,
        MONTH(hire_date) AS hire_date_month,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM employees
    WHERE LENGTH(full_name) >= 14
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        hire_date,
        is_remote,
        level,
        hire_date_month,
        job_title_clean,
        SUM(hire_date_month) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_hire_date_month,
        SUM(hire_date_month) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_hire_date_month
    FROM base
),
with_dims AS (
    -- Attach departments attributes
    SELECT e.*, d.location
    FROM metrics e
    LEFT JOIN departments d
        ON e.dept_id = d.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY dept_id, manager_id ORDER BY running_hire_date_month DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY cumulative_hire_date_month DESC, id) AS seq_no_2,
        ROW_NUMBER() OVER (ORDER BY cumulative_hire_date_month, id) AS row_seq,
        AVG(cumulative_hire_date_month) OVER (ORDER BY cumulative_hire_date_month, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cumulative_hire_date_month_avg_3
    FROM with_dims
),
kept_keys AS (
    -- Set difference on keys
    SELECT level, manager_id
    FROM with_windows
    EXCEPT
    SELECT level, manager_id
    FROM employees
    WHERE job_title = 'Manager'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.level = k.level AND s.manager_id = k.manager_id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        manager_id,
        hire_date,
        is_remote,
        level,
        hire_date_month,
        job_title_clean,
        cumulative_hire_date_month,
        location,
        seq_no_2,
        row_seq,
        cumulative_hire_date_month_avg_3,
        LENGTH(level) AS level_length,
        ROUND(row_seq, 2) AS row_seq_rounded
    FROM verified
    WHERE location IS NOT NULL
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            manager_id,
            level,
            cumulative_hire_date_month
        FROM valid
    ) src
    PIVOT (
        AVG(cumulative_hire_date_month) AS amount,
        COUNT(1) AS item_count
        FOR level IN ('L5', 'L2', 'L1')
    )
)
SELECT
    s.manager_id,
    s.L5_amount,
    s.L2_amount,
    s.L2_item_count,
    s.L1_item_count,
    (SELECT AVG(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS avg_employee_salary
FROM by_type s;
